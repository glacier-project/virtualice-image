// Pallet motion for ICE Teach driven by the ice-frost conveyor model.
//
// DrivePallet.Update is patched (frost_twin/Injector) to call Driver.Drive first and to return when
// it answers true: Unity's own routing, speed inference and collision blocking are then skipped and
// every pallet only follows the commands that Frost publishes on Kafka topic ice_data_conveyor_twin,
// one JSON object per message:
//
//   {"p": 7, "pts": [[x, z] or [x, y, z], ...], "d": 2.0}             (+ "snap": true)
//   {"p": 7, "content": [0, 11, 12], "color": "yellow"}               (Parts.cs)
//
// pallet 7 goes from where it is now through the points (pallet-parent local coordinates; a point
// without y keeps the height of the previous one) and
// reaches the last one d seconds after the message arrives, at constant speed along the path.
// A new command replaces the running one and starts from the current position; d = 0 places the
// pallet at the last point (initial layout). "snap": when the motion ends the QC camera photographs the
// pallet (Qc.cs). Content messages redraw what lies on the pallet (Parts.cs).
//
// Mode from the file <build>/ICE Teach_Data/FrostTwin.enabled: "drive" (above), "trace" (DrivePallet
// moves the pallets as in the original build and every position change is appended to
// FrostTwin.trace.csv: time, pallet, x, y, z, used to measure the scene paths). No file: original build.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Reflection;
using System.Threading;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using UnityEngine;

namespace FrostTwin
{
    public static class Driver
    {
        public const string Topic = "ice_data_conveyor_twin";

        sealed class Motion
        {
            public Vector3[] Pts;   // Pts[0] = position when the command was applied
            public float[] Cum;     // cumulative path length at each point
            public float T0, D;
            public bool Snap;       // photograph the pallet when the motion ends

            public Vector3 At(float now)
            {
                float total = Cum[Cum.Length - 1];
                if (D <= 0f || total <= 0f) return Pts[Pts.Length - 1];
                float s = Mathf.Clamp01((now - T0) / D) * total;
                int i = 1;
                while (i < Cum.Length - 1 && Cum[i] < s) i++;
                float seg = Cum[i] - Cum[i - 1];
                return seg > 0f ? Vector3.Lerp(Pts[i - 1], Pts[i], (s - Cum[i - 1]) / seg) : Pts[i];
            }
        }

        static readonly ConcurrentQueue<string> Inbox = new ConcurrentQueue<string>();
        static readonly Dictionary<int, Queue<JObject>> Pending = new Dictionary<int, Queue<JObject>>();
        static readonly Dictionary<int, Motion> Motions = new Dictionary<int, Motion>();
        static readonly Dictionary<int, JObject> NewContent = new Dictionary<int, JObject>();  // to draw
        static readonly Dictionary<int, JObject> Content = new Dictionary<int, JObject>();     // drawn
        static int state;  // 0 not started, 1 drive, 2 trace, -1 off
        static StreamWriter trace;
        static readonly Dictionary<int, Vector3> Traced = new Dictionary<int, Vector3>();
        static int lastFrame = -1;
        static volatile bool stop;

        /// Called by DrivePallet.Update. True: Frost moved the pallet, skip the original Update.
        public static bool Drive(Transform pallet, int palletId)
        {
            if (state == 0) Start();
            if (state == 2) { Trace(pallet, palletId); return false; }
            if (state < 0 || pallet == null) return state > 0;
            if (Time.frameCount != lastFrame) { lastFrame = Time.frameCount; Drain(); }
            Qc.Prepare();
            JObject c;
            if (NewContent.TryGetValue(palletId, out c))
            {
                NewContent.Remove(palletId);
                Content[palletId] = c;
                Parts.Show(pallet, c);
            }

            float now = Time.time;
            Motion m;
            Motions.TryGetValue(palletId, out m);
            Queue<JObject> q;
            if (Pending.TryGetValue(palletId, out q))
            {
                while (q.Count > 0)
                {
                    Vector3 from = m != null ? m.At(now) : pallet.localPosition;
                    m = Build(q.Dequeue(), from, now);
                }
                Motions[palletId] = m;
            }
            if (m != null) pallet.localPosition = m.At(now);
            if (m != null && m.Snap && now >= m.T0 + m.D)
            {
                m.Snap = false;
                Content.TryGetValue(palletId, out c);
                Qc.Shoot(pallet, palletId, c);
            }
            return true;
        }

        static void Trace(Transform pallet, int id)
        {
            if (pallet == null) return;
            Vector3 p = pallet.localPosition, last;
            if (Traced.TryGetValue(id, out last) && (p - last).sqrMagnitude < 1e-6f) return;
            Traced[id] = p;
            trace.WriteLine(string.Format(System.Globalization.CultureInfo.InvariantCulture,
                "{0:F3},{1},{2:F4},{3:F4},{4:F4}", Time.time, id, p.x, p.y, p.z));
            if (Time.frameCount % 120 == 0) trace.Flush();
        }

        static Motion Build(JObject cmd, Vector3 from, float now)
        {
            var pts = new List<Vector3> { from };
            foreach (JToken p in (JArray)cmd["pts"])
            {
                var a = (JArray)p;
                float y = a.Count == 3 ? (float)a[1] : pts[pts.Count - 1].y;
                pts.Add(new Vector3((float)a[0], y, (float)a[a.Count - 1]));
            }
            var cum = new float[pts.Count];
            for (int i = 1; i < pts.Count; i++) cum[i] = cum[i - 1] + Vector3.Distance(pts[i - 1], pts[i]);
            return new Motion { Pts = pts.ToArray(), Cum = cum, T0 = now, D = (float)cmd["d"], Snap = (bool?)cmd["snap"] == true };
        }

        static void Drain()
        {
            string raw;
            while (Inbox.TryDequeue(out raw))
            {
                try
                {
                    JObject cmd = JObject.Parse(raw);
                    int p = (int)cmd["p"];
                    if (cmd["content"] != null) { NewContent[p] = cmd; continue; }
                    Queue<JObject> q;
                    if (!Pending.TryGetValue(p, out q)) Pending[p] = q = new Queue<JObject>();
                    q.Enqueue(cmd);
                }
                catch (Exception e) { Debug.LogWarning("FROST TWIN: bad command " + raw + ": " + e.Message); }
            }
        }

        static void Start()
        {
            state = -1;
            string flag = Path.Combine(Application.dataPath, "FrostTwin.enabled");
            if (!File.Exists(flag)) return;
            if (File.ReadAllText(flag).Trim() == "trace")
            {
                trace = new StreamWriter(Path.Combine(Application.dataPath, "FrostTwin.trace.csv"), false);
                Application.quitting += () => trace.Close();
                state = 2;
                Debug.Log("FROST TWIN: tracing pallet positions");
                return;
            }
            if (Endpoints.Lab) { Debug.LogWarning("FROST TWIN: -lab, the real line has no " + Topic + ": pallets left to DrivePallet"); return; }
            // same broker (Endpoints) and credentials as the build's KafkaManager (scene fields)
            Type km = Type.GetType("KafkaManager, Assembly-CSharp");
            UnityEngine.Object mgr = km != null ? UnityEngine.Object.FindAnyObjectByType(km) : null;
            if (mgr == null) { Debug.LogError("FROST TWIN: no KafkaManager in the scene, pallets left to DrivePallet"); return; }
            const BindingFlags F = BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
            Func<string, string> field = n => km.GetField(n, F) != null ? km.GetField(n, F).GetValue(mgr) as string : null;
            var cfg = new ConsumerConfig
            {
                BootstrapServers = Endpoints.KafkaServers,
                GroupId = "frost-twin",
                EnableAutoCommit = false,
                SecurityProtocol = SecurityProtocol.SaslPlaintext,
                SaslMechanism = SaslMechanism.ScramSha512,
                SaslUsername = field("username"),
                SaslPassword = field("password"),
            };
            Qc.Init(new ProducerConfig
            {
                BootstrapServers = cfg.BootstrapServers, SecurityProtocol = cfg.SecurityProtocol, SaslMechanism = cfg.SaslMechanism,
                SaslUsername = cfg.SaslUsername, SaslPassword = cfg.SaslPassword,
            });
            var t = new Thread(() => Consume(cfg)) { IsBackground = true, Name = "FrostTwin" };
            t.Start();
            Application.quitting += () => stop = true;
            state = 1;
        }

        static void Consume(ConsumerConfig cfg)
        {
            try
            {
                using (var c = new ConsumerBuilder<Ignore, string>(cfg).Build())
                {
                    // no group join: start at the end of partition 0, Frost publishes the layout after this
                    c.Assign(new TopicPartitionOffset(Topic, 0, Offset.End));
                    bool ready = false;
                    while (!stop)
                    {
                        ConsumeResult<Ignore, string> r = c.Consume(TimeSpan.FromMilliseconds(100));
                        if (!ready) { ready = true; Debug.Log("FROST TWIN: ready on " + Topic); }
                        if (r != null && r.Message != null) Inbox.Enqueue(r.Message.Value);
                    }
                    c.Close();
                }
            }
            catch (Exception e) { Debug.LogError("FROST TWIN: consumer stopped: " + e); }
        }
    }
}
