// Hooks FrostTwin into ICE Teach (Assembly-CSharp.dll rewritten in place; idempotent):
//   DrivePallet.Update             if (FrostTwin.Driver.Drive(this.pallet.transform, this.palletID)) return;
//   KafkaManager.ConsumeMessage    Endpoints.Assigned(new ConsumerBuilder(Endpoints.Kafka(cfg)))
//   KafkaManager.CheckKafkaConnection  new AdminClientBuilder(Endpoints.Kafka(cfg))
//   RabbitClient.Start             Endpoints.Rabbit(factory).CreateConnection()
//   RabbitClient.RpcWrite          if (!Endpoints.AllowWrite(variable)) return Endpoints.Refused();
// usage: Injector <Managed dir>
using System;
using System.IO;
using System.Linq;
using Mono.Cecil;
using Mono.Cecil.Cil;

class Program
{
    static int Main(string[] args)
    {
        string managed = args[0];
        string target = Path.Combine(managed, "Assembly-CSharp.dll");
        var resolver = new DefaultAssemblyResolver();
        resolver.AddSearchDirectory(managed);
        var rp = new ReaderParameters { AssemblyResolver = resolver, ReadingMode = ReadingMode.Immediate };
        var asm = AssemblyDefinition.ReadAssembly(target, rp);
        var twin = AssemblyDefinition.ReadAssembly(Path.Combine(managed, "FrostTwin.dll"), rp);
        var mod = asm.MainModule;
        Func<string, string, MethodReference> twinM = (type, name) =>
            mod.ImportReference(twin.MainModule.GetType("FrostTwin." + type).Methods.Single(m => m.Name == name));
        Func<string, string, MethodDefinition> method = (type, name) => mod.GetType(type).Methods.Single(m => m.Name == name);

        var dp = mod.GetType("DrivePallet");
        var update = dp.Methods.Single(m => m.Name == "Update" && !m.HasParameters);
        if (update.Body.Instructions.Take(8).Any(i => i.Operand is MethodReference r && r.DeclaringType.FullName == "FrostTwin.Driver"))
        {
            Console.WriteLine("already hooked");
            return 0;
        }

        // DrivePallet.Update
        var palletF = dp.Fields.Single(f => f.Name == "pallet");
        var idF = dp.Fields.Single(f => f.Name == "palletID");
        var transformF = palletF.FieldType.Resolve().Fields.Single(f => f.Name == "transform");
        if (idF.FieldType.FullName != "System.Int32") { Console.Error.WriteLine("palletID is not int"); return 1; }
        var il = update.Body.GetILProcessor();
        var first = update.Body.Instructions[0];
        foreach (var ins in new[]
        {
            il.Create(OpCodes.Ldarg_0), il.Create(OpCodes.Ldfld, palletF), il.Create(OpCodes.Ldfld, mod.ImportReference(transformF)),
            il.Create(OpCodes.Ldarg_0), il.Create(OpCodes.Ldfld, idF),
            il.Create(OpCodes.Call, twinM("Driver", "Drive")), il.Create(OpCodes.Brfalse, first), il.Create(OpCodes.Ret),
        }) il.InsertBefore(first, ins);

        // Kafka and RabbitMQ endpoints: calls `T Endpoints.X(T)` around the one instruction that uses the object
        var kafka = twinM("Endpoints", "Kafka");
        Hook(method("KafkaManager", "ConsumeMessage"), "ConsumerBuilder`2::.ctor", kafka, twinM("Endpoints", "Assigned"));
        Hook(method("KafkaManager", "CheckKafkaConnection"), "AdminClientBuilder::.ctor", kafka, null);
        Hook(method("RabbitClient", "Start"), "ConnectionFactory::CreateConnection", twinM("Endpoints", "Rabbit"), null);

        // RabbitClient.RpcWrite(queue, variable, value, timeout): async stub, the guard goes before the state machine
        var write = method("RabbitClient", "RpcWrite");
        il = write.Body.GetILProcessor();
        first = write.Body.Instructions[0];
        foreach (var ins in new[]
        {
            il.Create(OpCodes.Ldarg_2), il.Create(OpCodes.Call, twinM("Endpoints", "AllowWrite")), il.Create(OpCodes.Brtrue, first),
            il.Create(OpCodes.Call, twinM("Endpoints", "Refused")), il.Create(OpCodes.Ret),
        }) il.InsertBefore(first, ins);

        string tmp = target + ".frosttwin.tmp";
        asm.Write(tmp);
        asm.Dispose(); twin.Dispose();
        File.Copy(tmp, target, true);
        File.Delete(tmp);
        Console.WriteLine("hooked: DrivePallet.Update, KafkaManager.ConsumeMessage/CheckKafkaConnection, RabbitClient.Start/RpcWrite");
        return 0;
    }

    // The single newobj/call `Type::Name` in m: `before` gets its argument (config object on the stack),
    // `after` its result.
    static void Hook(MethodDefinition m, string site, MethodReference before, MethodReference after)
    {
        var hits = m.Body.Instructions.Where(i => (i.OpCode == OpCodes.Newobj || i.OpCode == OpCodes.Call || i.OpCode == OpCodes.Callvirt)
            && i.Operand is MethodReference r && r.DeclaringType.Name + "::" + r.Name == site).ToList();
        if (hits.Count != 1) throw new Exception($"{m.FullName}: {hits.Count} '{site}' sites, expected 1 (different build?)");
        var il = m.Body.GetILProcessor();
        if (before != null) il.InsertBefore(hits[0], il.Create(OpCodes.Call, before));
        if (after != null) il.InsertAfter(hits[0], il.Create(OpCodes.Call, after));
    }
}
