// Brokers of ICE Teach, chosen on the command line. frost_twin/Injector hooks these methods into
// KafkaManager (ConsumeMessage, CheckKafkaConnection) and RabbitClient (Start, RpcWrite):
//
//   "ICE Teach.exe"        local virtualice-image stack: Kafka localhost:9093, RabbitMQ 127.0.0.1:30690
//   "ICE Teach.exe" -lab   the real ICE line: Kafka 10.197.21.10:30529, RabbitMQ 10.197.21.10:30690;
//                          RPC reads only, every RPC write (bay5Light reset, destination field) is refused
//
// Both: a consumer group of its own for every run, starting at the end of the topics, no committed
// offsets. The stock build used the fixed group unity-digitalshadow-group from the earliest offset:
// a restart replayed everything since the last commit, and a second instance (e.g. the lab's own
// digital shadow) split the partitions with it, so each one saw only part of the line.
// A group without committed offsets is dropped by the broker when the run ends.
using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using RabbitMQ.Client;
using UnityEngine;

namespace FrostTwin
{
    public static class Endpoints
    {
        public static readonly bool Lab = Array.IndexOf(Environment.GetCommandLineArgs(), "-lab") >= 0;
        public static readonly string KafkaServers = Lab ? "10.197.21.10:30529" : "localhost:9093";
        static readonly string RabbitHost = Lab ? "10.197.21.10" : "127.0.0.1";
        static readonly string Group = "unity-digitalshadow-" + Environment.MachineName.ToLowerInvariant() + "-" + Guid.NewGuid().ToString("N").Substring(0, 8);

        /// Before `new ConsumerBuilder(cfg)` / `new AdminClientBuilder(cfg)` in KafkaManager.
        public static ClientConfig Kafka(ClientConfig c)
        {
            c.BootstrapServers = KafkaServers;
            var cc = c as ConsumerConfig;
            if (cc != null)
            {
                cc.GroupId = Group;
                cc.AutoOffsetReset = AutoOffsetReset.Latest;
                cc.EnableAutoCommit = false;
                Debug.Log("ICE TEACH: Kafka " + KafkaServers + (Lab ? " (real line)" : " (local stack)") + ", group " + Group);
            }
            else if (!Lab) CreateMissingTopics(c);  // CheckKafkaConnection runs before the consumer subscribes
            return c;
        }

        // Local stack: a topic Unity subscribes to that does not exist yet is only found by the metadata
        // refresh (5 min), so the first minutes of a replay on a fresh stack were lost. Creates them now.
        static void CreateMissingTopics(ClientConfig c)
        {
            try
            {
                var topics = (string[])Type.GetType("KafkaManager, Assembly-CSharp")
                    .GetField("topics", BindingFlags.Static | BindingFlags.NonPublic).GetValue(null);
                using (var admin = new AdminClientBuilder(c).Build())
                {
                    var have = new HashSet<string>(admin.GetMetadata(TimeSpan.FromSeconds(5)).Topics.Where(t => !t.Error.IsError).Select(t => t.Topic));
                    var missing = topics.Where(t => !have.Contains(t))
                        .Select(t => new TopicSpecification { Name = t, NumPartitions = 1, ReplicationFactor = 1 }).ToList();
                    if (missing.Count == 0) return;
                    admin.CreateTopicsAsync(missing).Wait();
                    Debug.Log("ICE TEACH: created " + missing.Count + " missing topics on the local stack");
                }
            }
            catch (Exception e) { Debug.LogWarning("ICE TEACH: cannot create the missing topics: " + e.GetBaseException().Message); }
        }

        /// After `new ConsumerBuilder(cfg)` in KafkaManager.ConsumeMessage: the scripts wait for this line.
        public static ConsumerBuilder<Ignore, string> Assigned(ConsumerBuilder<Ignore, string> b)
        {
            return b.SetPartitionsAssignedHandler((c, tps) => Debug.Log("ICE TEACH: Kafka assigned " + tps.Count + " partitions"));
        }

        /// Before `factory.CreateConnection()` in RabbitClient.Start.
        public static ConnectionFactory Rabbit(ConnectionFactory f)
        {
            f.HostName = RabbitHost;
            Debug.Log("ICE TEACH: RabbitMQ " + RabbitHost + ":" + f.Port + (Lab ? " (real line, read only)" : " (local stack)"));
            return f;
        }

        /// First thing in RabbitClient.RpcWrite: false -> RpcWrite returns Refused() without sending.
        public static bool AllowWrite(string variable)
        {
            if (!Lab) return true;
            Debug.LogWarning("ICE TEACH: -lab, RPC write to " + variable + " refused (read only on the real line)");
            return false;
        }

        public static Task<bool> Refused() { return Task.FromResult(false); }
    }
}
