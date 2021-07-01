//============================================================
// Project: TimeTunnel
// Author: Zoranner@ZORANNER
// Datetime: 2019-05-14 10:38:47
// Description: Base on More Effective Coroutines Threaded v3.08.0
//============================================================

using UnityEngine;
using System.Threading;
using System.Collections.Generic;

namespace Zoranner.SuperCoroutines
{
    public class ThreadTunnel : MonoBehaviour
    {
        public const float SwitchBackToGUIThread = 0f;
        public const float YieldToOtherTasksOnThisThread = -42f;

        private readonly CoroutineHandle _coroutineKey = new CoroutineHandle(0);
        private readonly Queue<System.Exception> _exceptions = new Queue<System.Exception>();
        private readonly Queue<Process> _returningProcesses = new Queue<Process>();
        private readonly List<ThreadData>[] _threadSpindles = new List<ThreadData>[5];
        private readonly List<ThreadData> _looseThreads = new List<ThreadData>();
        private readonly Queue<ThreadData> _abandonedThreads = new Queue<ThreadData>();
        private readonly object _returnLock = new object();

        private volatile static ThreadData _chosenThread;

        private const int ProcessesBlockSize = 32;
		private bool initialized = false;

        private static ThreadTunnel _instance;
        private static ThreadTunnel Instance
        {
            get
            {
                if (_instance == null)
                {
                    GameObject instanceHome = GameObject.Find("TimeTunnel Controller");

					if (instanceHome == null) {
						instanceHome = new GameObject { name = "TimeTunnel Controller" };
#if UNITY_EDITOR
						if (Application.isPlaying)
							DontDestroyOnLoad (instanceHome);
#else
                        DontDestroyOnLoad(instanceHome);
#endif
					}

                    _instance = instanceHome.GetComponent<ThreadTunnel>() ?? instanceHome.AddComponent<ThreadTunnel>();

					_instance.Initialize();
                }

                return _instance;
            }
        }

		void Awake()
		{
			if (_instance == null)
				_instance = this;

			Initialize();
		}

        private void Initialize()
        {
			if (!initialized) 
			{
				for (int i = 0; i < 5; i++)
					_threadSpindles [i] = new List<ThreadData> (SystemInfo.processorCount);

				TimeTunnel.OnPreExecute += ReturnAllProcesses;

				initialized = true;
			}
        }

        void OnDestroy()
        {
            for (int i = 4;i >= 0;i--)
            {
                for (int j = 0;j < _threadSpindles[i].Count;j++)
                {
                    _threadSpindles[i][j].Running = false;
                    lock (_threadSpindles[i][j].RunLock) { Monitor.Pulse(_threadSpindles[i][j].RunLock); }
                }
                _threadSpindles[i].Clear();
            }

            for (int i = 0;i < _looseThreads.Count;i++)
            {
                _looseThreads[i].Running = false;
                lock (_looseThreads[i].RunLock) { Monitor.Pulse(_looseThreads[i].RunLock); }
            }
            _looseThreads.Clear();

            TimeTunnel.OnPreExecute -= ReturnAllProcesses;
            _instance = null;
        }

        void Update()
        {
            while (_exceptions.Count > 0)
            {
                System.Exception ex = _exceptions.Dequeue();
                Debug.LogError(ex.Message + "\n" + ex.StackTrace);
            }

            while (_abandonedThreads.Count > 0)
            {
                _chosenThread = _abandonedThreads.Dequeue();

                if (Monitor.TryEnter(_chosenThread.RunLock, 10))
                {
                    Monitor.PulseAll(_chosenThread.RunLock);
                    Monitor.Exit(_chosenThread.RunLock);
                }
                else
                {
                    Instance._abandonedThreads.Enqueue(_chosenThread);
                    return;
                }
            }
        }

        /// <summary>
        /// Use "yield return ThreadTunnel.SwitchToExternalThread();" to execute the next block in an external thread.
        /// </summary>
        /// <param name="priority">The thread priority. In most cases it's best to leave this at Normal.</param>
        public static float SwitchToExternalThread(UnityEngine.ThreadPriority priority)
        {
            if ((int)priority >= 5 || (int)priority < 0)
            {
                Debug.LogError("Thread priority out of range.");
                return 0;
            }

            SelectThreadFromSpindle((System.Threading.ThreadPriority)priority);
            TimeTunnel.ReplacementFunction = RetrieveCoroutine;

            return float.NaN;
        }

        /// <summary>
        /// Use "yield return ThreadTunnel.SwitchToExternalThread();" to execute the next block in an external thread.
        /// </summary>
        /// <param name="priority">The thread priority. In most cases it's best to leave this at Normal.</param>
        public static float SwitchToExternalThread(System.Threading.ThreadPriority priority = System.Threading.ThreadPriority.Normal)
        {
            if ((int)priority >= 5 || (int)priority < 0)
            {
                Debug.LogError("Thread priority out of range.");
                return 0;
            }

            SelectThreadFromSpindle(priority);
            TimeTunnel.ReplacementFunction = RetrieveCoroutine;

            return float.NaN;
        }

        /// <summary>
        /// Use "yield return ThreadTunnel.SwitchToDedicatedExternalThread();" to execute the next block in an external thread. Dedicated threads should be used
        /// when you expect the code block to take longer than one frame to run.
        /// </summary>
        /// <param name="priority">The thread priority. In most cases it's best to leave this at Normal.</param>
        public static float SwitchToDedicatedExternalThread(UnityEngine.ThreadPriority priority)
        {
            if ((int)priority >= 5 || (int)priority < 0)
            {
                Debug.LogError("Thread priority out of range.");
                return 0;
            }

            SelectLooseThread((System.Threading.ThreadPriority)priority);
            TimeTunnel.ReplacementFunction = RetrieveCoroutine;

            return float.NaN;
        }

        /// <summary>
        /// Use "yield return ThreadTunnel.SwitchToDedicatedExternalThread();" to execute the next block in an external thread. Dedicated threads should be used
        /// when you expect the code block to take longer than one frame to run.
        /// </summary>
        /// <param name="priority">The thread priority. In most cases it's best to leave this at Normal.</param>
         public static float SwitchToDedicatedExternalThread(System.Threading.ThreadPriority priority = System.Threading.ThreadPriority.Normal)
        {
            if ((int)priority >= 5 || (int)priority < 0)
            {
                Debug.LogError("Thread priority out of range.");
                return 0;
            }

            SelectLooseThread(priority);
            TimeTunnel.ReplacementFunction = RetrieveCoroutine;

            return float.NaN;
        }

        private static IEnumerator<float> RetrieveCoroutine(IEnumerator<float> coptr, CoroutineHandle handle) 
        {
            lock (_chosenThread.ProcessLock) { AddItemToProcessQueue(_chosenThread, new Process { Handle = handle, Enumerator = coptr }); }

            if (Monitor.TryEnter(_chosenThread.RunLock, 8))
            {
                Monitor.PulseAll(_chosenThread.RunLock);
                Monitor.Exit(_chosenThread.RunLock);
            }
            else
            {
                Instance._abandonedThreads.Enqueue(_chosenThread);
            }

            TimeTunnel.GetInstance(handle.Key).LockCoroutine(handle, Instance._coroutineKey);
            return coptr;
        }

        private static void AddItemToProcessQueue(ThreadData chosenThread, Process process)
        {
            if ((chosenThread.Head + 1) % chosenThread.Processes.Length == chosenThread.Tail)
            {
                Process[] newProcesses = new Process[chosenThread.Processes.Length + ProcessesBlockSize];
                int j = 0;
                int i;
                for (i = chosenThread.Tail;i != chosenThread.Head;i = (i + 1) % chosenThread.Processes.Length)
                    newProcesses[j++] = chosenThread.Processes[i];

                newProcesses[j] = chosenThread.Processes[i];

                chosenThread.Head = j;
                chosenThread.Tail = 0;
                chosenThread.Processes = newProcesses;
            }

            chosenThread.Processes[chosenThread.Head] = process;
            chosenThread.Head = (chosenThread.Head + 1) % chosenThread.Processes.Length;
        }

        private static void SelectThreadFromSpindle(System.Threading.ThreadPriority priority)
        {
			
            List<ThreadData> spindle = Instance._threadSpindles[(int)priority];

            if (spindle.Count < SystemInfo.processorCount)
            {
                for (int i = 0;i < spindle.Count;i++)
                {
                    if (!spindle[i].Running)
                    {
                        spindle.RemoveAt(i);
                        i = 0;
                        continue;
                    }

                    if (spindle[i].Head == spindle[i].Tail)
                    {
                        _chosenThread = spindle[i];
                        return;
                    }
                }

                _chosenThread = new ThreadData
                {
                    Running = true,
                    Thread = new Thread(ThreadProcess)
                    {
                        IsBackground = true,
                        Priority = priority
                    }
                };

                spindle.Add(_chosenThread);
                _chosenThread.Thread.Start(_chosenThread);
            }

            int bestThreadCount = int.MaxValue;
            _chosenThread = null;

            for (int i = 0;i < spindle.Count;i++)
            {
                if (!spindle[i].Running)
                {
                    spindle.RemoveAt(i);
                    i = 0;
                    continue;
                }

                int count = spindle[i].Head - spindle[i].Tail;
                if (count < 0)
                    count += spindle[i].Processes.Length;

                if (count < bestThreadCount)
                {
                    _chosenThread = spindle[i];
                    bestThreadCount = count;
                }
            }
        }

        private static void SelectLooseThread(System.Threading.ThreadPriority priority)
        {
            for (int i = 0;i < Instance._looseThreads.Count;i++)
            {
                if (!Instance._looseThreads[i].Running)
                {
                    Instance._looseThreads.RemoveAt(i);
                    i = 0;
                    continue;
                }

                if (Instance._looseThreads[i].Head == Instance._looseThreads[i].Tail)
                {
                    Instance._looseThreads[i].Thread.Priority = priority;
                    _chosenThread = Instance._looseThreads[i];
                    return; 
                }
            }

            _chosenThread = new ThreadData
            {
                Running = true,
                Thread = new Thread(ThreadProcess)
                {
                    IsBackground = true,
                    Priority = priority
                }
            };

            Instance._looseThreads.Add(_chosenThread);
            _chosenThread.Thread.Start(_chosenThread);
        }

        private static void ThreadProcess(object input)
        {
            if (!(input is ThreadData))
                return;
            ThreadData threadData = (ThreadData)input;
            Process process = new Process();

            try
            {
                while(threadData.Running)
                {
                    process.Enumerator = null;
                    lock (threadData.RunLock) 
                    {
                        if (threadData.Head == threadData.Tail)
                            Monitor.Wait(threadData.RunLock); 

                        lock (threadData.ProcessLock)
                        {
                            if (threadData.Head != threadData.Tail)
                            {
                                process = threadData.Processes[threadData.Tail];
                                threadData.Tail = (threadData.Tail + 1) % threadData.Processes.Length;
                            }
                        }

                        if (process.Enumerator == null) continue;
                    }

                    bool done;
                    try
                    {
                        done = !process.Enumerator.MoveNext();
                        done |= process.Enumerator.Current == SwitchBackToGUIThread;
                    }
                    catch (System.Exception ex)
                    {
                        process.Exception = ex;
                        lock (Instance._returnLock) { Instance._returningProcesses.Enqueue(process); }
                        continue;
                    }

                    if (done)
                    {
                        lock (Instance._returnLock) { Instance._returningProcesses.Enqueue(process); }
                    }
                    else
                    {
                        if (process.Enumerator.Current == YieldToOtherTasksOnThisThread)
                            lock (threadData.ProcessLock) { AddItemToProcessQueue(threadData, process); }
                        else
                            lock (Instance._returnLock) { Instance._returningProcesses.Enqueue(process); }
                    }
                }
            }
            catch
            {
                threadData.Running = false;
            }
        }

        private void ReturnAllProcesses()
        {
            if (Monitor.TryEnter(_returnLock, 8))
            {
                while (_returningProcesses.Count > 0)
                {
                    ReturnProcess(_returningProcesses.Dequeue());
                }
                Monitor.Exit(_returnLock);
            }
        }

        private void ReturnProcess(Process process)
        {
            if (process.Exception != null)
            {
                TimeTunnel.KillCoroutines(process.Handle);
                _exceptions.Enqueue(process.Exception);
            }
            else
            {
                TimeTunnel.GetInstance(process.Handle.Key).UnlockCoroutine(process.Handle, _coroutineKey);
            }
         }

        /// <summary>
        /// Puts the current thread to sleep for a set amount of time.
        /// </summary>
        /// <param name="seconds">The number of seconds that the thread should sleep.</param>
        public static void Sleep(float seconds)
        {
            if(Thread.CurrentThread == TimeTunnel.MainThread)
                throw new System.InvalidOperationException("Sleep was called while on the GUI thread.");

            Thread.Sleep((int)(seconds * 1000f));
        }


        private struct Process
        {
            public CoroutineHandle Handle;
            public IEnumerator<float> Enumerator;
            public System.Exception Exception;
        }

        private class ThreadData
        {
            public bool Running;
            public Thread Thread;
            public readonly object RunLock = new object();
            public readonly object ProcessLock = new object();
            public Process[] Processes = new Process[ProcessesBlockSize];
            public int Head;
            public int Tail;
        }
    }
}
