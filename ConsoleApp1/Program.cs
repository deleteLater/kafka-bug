using ConsoleApp1;

Console.WriteLine("Hello World!");

var consumer = new Consumer();
Task.Run(() => consumer.StartAsync());

var producer = new Producer();

Console.WriteLine("Produce message for topic A.");
await producer.ProduceAsync("A", "A value");

Console.WriteLine("Press any key to continue...");
Console.ReadKey();

Console.WriteLine("Produce message for topic B.");
await producer.ProduceAsync("B", "B value");

Console.WriteLine("Press any key to continue...");
Console.ReadKey();