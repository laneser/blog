---
title: "BlockingCollection in C#"
date: 2013-02-27T11:49:45+08:00
draft: false
comments: true
categories: ["CSharp"]

---

看到 BlockingCollection 這個 class,

Producer-consumer pattern 用它來寫實在是太愉悅了.

原本是需要 lock queue, 還要在拿不到東西的時候進行 wait event handler 等等動作,
現在只要短短幾行

```c#
foreach (var data in queue.GetConsumingEnumerable(cts.Token))
{
    try
    {
        writefunc(data);
    }
    catch (Exception ex)
    {
        logger.Fatal(ex.ToString());
    }
}
```				

解決了!

如果要加上 multi-thread consumer, 太 easy!

```
Parallel.ForEach(queue.GetConsumingEnumerable(cts.Token), data =>
{
    try
    {
        writefunc(data);
    }
    catch (Exception ex)
    {
        logger.Fatal(ex.ToString());
    }
});
```				


看看原本的 BatchWriterOld class code:

```
public class BatchWriterOld<TData> : IDisposable
{
    static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

    private Thread writerThread;
    private bool writethreadend = false;

    private List<TData> cache;
    private EventWaitHandle cacheWaitHandle;

    private TimeSpan waitgap;

    public delegate void BatchWrite(List<TData> data);

    private BatchWrite batchwritefunc;

    public BatchWriterOld(TimeSpan waitGap, BatchWrite writefunc)
    {
        waitgap = waitGap;
        batchwritefunc = writefunc;
        logger.Info("Batch writer started.");
        writethreadend = false;
        cache = new List<TData>();
        cacheWaitHandle = new EventWaitHandle(true, EventResetMode.ManualReset);
        writerThread = new Thread(() => writeThreadExecute());
        writerThread.Start();
    }

    public void AddData(IEnumerable<TData> data)
    {
        lock (cache)
        {
            cache.AddRange(data);
            cacheWaitHandle.Set();
        }
    }

    public void Flush()
    {
        var currentMutations = new List<TData>();
        lock (cache)
        {
            currentMutations.AddRange(cache);
            cache.Clear();
            cacheWaitHandle.Reset();
        }
        if (currentMutations.Count == 0)
            return;

        try
        {
            batchwritefunc(currentMutations);
        }
        catch (Exception ex)
        {
            logger.Fatal(ex.ToString());
        }
    }

    private void writeThreadExecute()
    {
        while (writethreadend == false)
        {
            try
            {
                Flush();
            }
            catch (Exception ex)
            {
                logger.Fatal(ex.ToString());
            }
            cacheWaitHandle.WaitOne(waitgap);
        }
    }

    public void Dispose()
    {
        //Flush();
    }
}
```

 跟新的 BatchWriter class.
 
```
public class BatchWriter<TData> : IDisposable
{
    private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private Task executeTask;
    private BlockingCollection<IEnumerable<TData>> queue;
    private Action<IEnumerable<TData>> batchwritefunc;
    private CancellationTokenSource cts;

    public BatchWriter(Action<IEnumerable<TData>> writefunc)
    {
        batchwritefunc = writefunc;
        logger.Info("Batch writer started.");
        queue = new BlockingCollection<IEnumerable<TData>>();
        cts = new CancellationTokenSource();
        executeTask = Task.Factory.StartNew(() =>
        {
            foreach (var data in queue.GetConsumingEnumerable(cts.Token))
            {
                try
                {
                    writefunc(data);
                }
                catch (Exception ex)
                {
                    logger.Fatal(ex.ToString());
                }
            }
        }, cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    public void AddData(IEnumerable<TData> data)
    {
        queue.Add(data);
    }

    public void Dispose()
    {
        try
        {
            queue.CompleteAdding();
        }
        catch (Exception ex)
        {
            logger.Fatal(ex.ToString());
        }
        logger.Info("Batch writer ended.");
    }
}
```


Multi-thread consumer version

```
public class BatchWriterMultiThread<TData> : IDisposable
{
    private static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();
    private Task executeTask;
    private BlockingCollection<IEnumerable<TData>> queue;
    private Action<IEnumerable<TData>> batchwritefunc;
    private CancellationTokenSource cts;

    public BatchWriterMultiThread(Action<IEnumerable<TData>> writefunc)
    {
        batchwritefunc = writefunc;
        logger.Info("Batch writer started.");
        queue = new BlockingCollection<IEnumerable<TData>>();
        cts = new CancellationTokenSource();
        executeTask = Task.Factory.StartNew(() =>
        {
            Parallel.ForEach(queue.GetConsumingEnumerable(cts.Token), data =>
            {
                try
                {
                    writefunc(data);
                }
                catch (Exception ex)
                {
                    logger.Fatal(ex.ToString());
                }
            });
        }, cts.Token, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    public void AddData(IEnumerable<TData> data)
    {
        queue.Add(data);
    }

    public void Dispose()
    {
        try
        {
            queue.CompleteAdding();
        }
        catch (Exception ex)
        {
            logger.Fatal(ex.ToString());
        }
        logger.Info("Batch writer ended.");
    }
}
```

測試用的 program

```
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ConsoleApplication1
{
    class Program
    {
        static NLog.Logger logger = NLog.LogManager.GetCurrentClassLogger();

        static void Main(string[] args)
        {
            Action<IEnumerable<String>> comsumer1 = (s) =>
            {
                foreach (var line in s)
                {
                    Console.WriteLine(string.Format("ba1 T{2} {0:HHmmss.ffff}: {1}", DateTime.Now, line, System.Threading.Thread.CurrentThread.ManagedThreadId));
                    Thread.Sleep(100);
                }
            };

            BatchWriterOld<string>.BatchWrite comsumer2 = (s) =>
            {
                foreach (var line in s)
                {
                    Console.WriteLine(string.Format("ba2 {0:HHmmss.ffff}: {1}", DateTime.Now, line));
                    Thread.Sleep(100);
                }
            };

            Action<IEnumerable<String>> comsumer3 = (s) =>
            {
                foreach (var line in s)
                {
                    Console.WriteLine(string.Format("ba3 T{2} {0:HHmmss.ffff}: {1}", DateTime.Now, line, System.Threading.Thread.CurrentThread.ManagedThreadId));
                    Thread.Sleep(100);
                }
            };


            using (var ba1 = new BatchWriter<String>(comsumer1))
            using (var ba2 = new BatchWriterOld<String>(TimeSpan.FromSeconds(5), comsumer2))
            using (var ba3 = new BatchWriterMultiThread<String>(comsumer3))
            {
                for (int i = 0; i < 10; i++)
                {
                    ba1.AddData(new string[] { i.ToString() });
                    ba2.AddData(new string[] { i.ToString() });
                    ba3.AddData(new string[] { i.ToString() });
                }

                Thread.Sleep(TimeSpan.FromSeconds(2));

                for (int i = 10; i < 20; i++)
                {
                    ba1.AddData(new string[] { i.ToString(), (i + 1).ToString() });
                    ba2.AddData(new string[] { i.ToString(), (i + 1).ToString() });
                    ba3.AddData(new string[] { i.ToString(), (i + 1).ToString() });
                }
            }

            Console.WriteLine("end");
            Console.ReadLine();
        }
    }
}
```


輸出結果

> ba1 T4 095127.9879: 0  
ba2 095127.9889: 0  
ba3 T6 095127.9899: 0  
ba3 T9 095127.9899: 3  
ba3 T12 095127.9899: 8  
ba3 T14 095127.9899: 7  
ba3 T13 095127.9899: 6  
ba3 T10 095127.9899: 4  
ba3 T11 095127.9899: 5  
ba3 T7 095127.9899: 1    
ba3 T8 095127.9899: 2  
ba2 095128.0900: 1  
ba1 T4 095128.0900: 1  
ba3 T12 095128.0910: 9  
ba2 095128.1910: 2  
ba1 T4 095128.1910: 2  
ba2 095128.2921: 3  
ba1 T4 095128.2921: 3  
ba2 095128.3921: 4  
ba1 T4 095128.3931: 4  
ba2 095128.4932: 5  
ba1 T4 095128.4942: 5  
ba2 095128.5943: 6  
ba1 T4 095128.5953: 6  
ba2 095128.6943: 7  
ba1 T4 095128.6963: 7  
ba2 095128.7954: 8  
ba1 T4 095128.7974: 8  
ba2 095128.8954: 9  
ba1 T4 095128.8984: 9  
ba3 T14 095129.9891: 10  
ba3 T8 095129.9891: 13  
ba3 T11 095129.9891: 14  
ba3 T6 095129.9891: 16  
ba3 T9 095129.9891: 17  
ba2 095129.9891: 10  
ba3 T13 095129.9891: 12  
ba3 T12 095129.9891: 18  
ba3 T16 095129.9891: 19  
ba1 T4 095129.9891: 10  
ba3 T7 095129.9891: 15  
ba3 T10 095129.9891: 11  
end  
ba3 T11 095130.0901: 15    
ba3 T12 095130.0901: 19  
ba3 T7 095130.0901: 16  
ba3 T16 095130.0901: 20  
ba3 T6 095130.0901: 17  
ba3 T9 095130.0901: 18  
ba3 T14 095130.0901: 11  
ba3 T13 095130.0901: 13  
ba3 T8 095130.0901: 14  
ba2 095130.0901: 11  
ba1 T4 095130.0901: 11  
ba3 T10 095130.0911: 12  
ba2 095130.1912: 11  
ba1 T4 095130.1912: 11  
ba2 095130.2922: 12  
ba1 T4 095130.2922: 12  
ba2 095130.3923: 12  
ba1 T4 095130.3933: 12  
ba2 095130.4934: 13  
ba1 T4 095130.4944: 13  
ba2 095130.5934: 13  
ba1 T4 095130.5944: 13  
ba2 095130.6945: 14  
ba1 T4 095130.6955: 14  
ba1 T4 095130.7955: 14  
ba2 095130.7955: 14  
ba2 095130.8966: 15  
ba1 T4 095130.8966: 15  
ba1 T4 095130.9976: 15  
ba2 095130.9976: 15  
ba1 T4 095131.0977: 16  
ba2 095131.0987: 16  
ba1 T4 095131.1988: 16  
ba2 095131.1998: 16  
ba2 095131.2998: 17  
ba1 T4 095131.2998: 17  
ba2 095131.4009: 17  
ba1 T4 095131.4009: 17  
ba2 095131.5009: 18  
ba1 T4 095131.5019: 18  
ba2 095131.6020: 18  
ba1 T4 095131.6020: 18  
ba2 095131.7020: 19  
ba1 T4 095131.7030: 19  
ba2 095131.8031: 19  
ba1 T4 095131.8031: 19  
ba2 095131.9042: 20  
ba1 T4 095131.9042: 20  


