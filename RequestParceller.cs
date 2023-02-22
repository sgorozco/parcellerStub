using log4net;
using Obsidian.Library.ClassExtensions;
using Obsidian.Library.Threading;
using Obsidian.Library.WCF.Properties;
using Obsidian.LogUtils;
using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;


namespace Obsidian.Library.WCF {
   // The RequestParceller related classes provide basic WCF parcelling request services.
   // Use these classes to reduce WCF chattiness by bundling multiple, related WCF service calls from multiple threads
   // into a single call.

   // IMPORTANT:
   // By default, logging to log4net is disabled. To enable logging set the UseLog4Net property to true. The reason for
   // this default configuration is that a common use for this class is parcelling log events that will be sent to a
   // remote viewer via WCF. If we log in this class, we will trigger an infinite cascade of logging events and will
   // degrade system performance greatly. If the parceller is not forwarding log events, it is safe to set this property
   // to True

   // To the client code, the call appears to be executed independently (the parcelling is fully hidden from client
   // code).
   // The parcelling policy is bounded by two conditions:  a maximum parcel size or a parcel-idle timeout. Whenever any
   // of the two conditions are reached, the parcel is sent to the WCF service as a single call

   // The provided WCF service inteface must expose a WCF operation that can receive a collection of multiple DTO
   // (data-transmission-objects) instances (one for each parcelled request).

   // The classes are abstract. A concrete implementation of the class must be supplied, overriding the
   // SendParcelToWCFService() method.  The SendParcelToWCFService() method must call the WCF service by means of the
   // _service member, passing the DTO objects available in the parcelled tuples.  After the WCF call is completed,
   // the overriden SendParcelToWCFService() method must set the results of the WCF call into the TaskCompletionSource
   // (TCO tuple member) objects in the corresponding parcelled tuples.  This action will resume all the multiple,
   // individual calls that are awaiting for their requests to be fulfilled.

   // Usage Example:
   //
   // public class AudioHashParceller : ParcellerBase<AudioHashDTO, AudioHashResultDTO, IAudioHashService> {
   //    protected async override Task SendParcelToWCFService(Parcel parcel, CancellationToken cancelToken) {
   //       // obtain a list of the DTO objects stored in the parcel tuples
   //       List<AudioHashDTO> hashes = parcel.Select(item => item.DTO).ToList();
   //       List<AudioHashResultDTO> results = await _service.ProcessMultipleHashes(hashes)
   //                                                        .ConfigureAwait(false);
   //       if( results.Count != hashes.Count )
   //          throw new Exception("expecting the same amount of results as inputs");
   //       for( int i = 0; i < results.Count; i++ )
   //          parcel[i].TCS.SetResult(results[i]);  // resume the method awaiting this result
   //    }
   // }
   //
   //   ...
   //   // Create a parceller that will ship (transmit) whenever 3 items are parcelled by different threads, or 1
   //   // second has elapsed from the last item bundled in the active parcel
   //   var audioHashParceller = new AudioHashParceller(wcfSevice,
   //                                                   maxParcelSize: 3,
   //                                                   idleShipTimeout: TimeSpan.FromSeconds(1.0));
   //
   //   AudioHashResultDTO result1 = await audioHashParceller.Request(GetAudioHashDTO());


   
   [EditorBrowsable(EditorBrowsableState.Never)] // Hide from Editor, we should override from RequestParceller<> classes
   public abstract class ParcellerBase<TRequestDTO, TResultDTO, TService> where TService : class {

      protected class ParcelTuple {
         public TRequestDTO requestDTO;
         public TaskCompletionSource<TResultDTO> TCS;
      }

      protected sealed class Parcel : List<ParcelTuple> { }

      protected static ILog _log;
      protected RobustServiceCaller<TService> _service;
      private AsyncLock _parcelLock;
      private Parcel _parcel;
      private volatile int _parcelMaxSize;  // 20180707 - support dynamic parcel size
      private TimeSpan _idleShipTimeout;
      private object _lastParcelEventTimestampLock;
      private DateTime _lpets;   // last parcel event timestamp, to be accessed via property only
      private AsyncManualResetEvent _parcelIsFullEvent;

      public string Name { get; }
      public bool UseLog4Net { get; set; }

      public int MaxParcelSize {
         get => _parcelMaxSize;
         set {
            if( value <= 0 )
               throw new InvalidOperationException(Resources.msgInvalidMaxParcelSize);
            _parcelMaxSize = value;
         }
      }

      public int ParcelSize {
         get {
            return _parcel.Count;
         }
      }

      static ParcellerBase() {
         _log = new DemystifyLogger(LogManager.GetLogger(nameof(ParcellerBase<TRequestDTO, TResultDTO, TService>)));
      }

      protected abstract Task SendParcelToWCFService(Parcel parcel, CancellationToken cancelToken);

      protected virtual void OnRequestAddedToParcel(TRequestDTO dto) {
      }

      private Task<TResultDTO> AddRequestToParcel(TRequestDTO dto, CancellationToken cancelToken) {
         cancelToken.ThrowIfCancellationRequested();
         this.LastParcelEventTimestamp = DateTime.UtcNow;
         
         var request = new ParcelTuple {
            requestDTO = dto,
            TCS = new TaskCompletionSource<TResultDTO>(TaskCreationOptions.RunContinuationsAsynchronously)
         };
         _parcel.Add(request);
         OnRequestAddedToParcel(dto);
         if( _parcel.Count >= _parcelMaxSize )   // 20180707 - support dynamic parcel size
            _parcelIsFullEvent.Set();
         return request.TCS.Task;
      }

      private async Task ReadyToShip(CancellationToken cancelToken) {
         TimeSpan extraTime = TimeSpan.FromMilliseconds(25);
         TimeSpan timeToDelay = _idleShipTimeout + extraTime;

         while( true ) {
            cancelToken.ThrowIfCancellationRequested();
            using( var cts = CancellationTokenSource.CreateLinkedTokenSource(cancelToken) ) {
               Task delay = Task.Delay(timeToDelay, cts.Token);
               Task parcelIsFull = _parcelIsFullEvent.WaitAsync();
               // delay WCF transmission until the parcel is full or the  maximum idle timeout is exceeded
               Task completedTask;
               try {
                  completedTask = await Task.WhenAny(parcelIsFull, delay)
                                            .AsyncTrace()
                                            .ConfigureAwait(false);
               } finally {
                  _parcelIsFullEvent.Reset();
               }
               if( completedTask == parcelIsFull ) {
                  cts.Cancel();  // cancel Delay task, preventing it from firing
                  // parcel is full and ready to be shipped (transmitted), note that since the parcel is unlocked,
                  // there exists a possibility that extra items are appended to the parcel before actual shipment occurs
                  if( _log.IsDebugEnabled ) {
                     string msg = $"{this.Name}: Parcel is full. Shipping now";
                     if( this.UseLog4Net )
                        _log.Debug(msg);
                     Debug.WriteLine(msg);
                  }
                  break;
               }
               // The timeout has elapsed, check if the parcel was in fact idle during this wait.
               DateTime utcNow = DateTime.UtcNow;
               DateTime lastParcelEventTimestamp = this.LastParcelEventTimestamp;
               TimeSpan idleTime = utcNow - lastParcelEventTimestamp;

               if( idleTime >= _idleShipTimeout ) {
                  // parcel was idle, ship now!
                  if( _log.IsDebugEnabled ) {
                     string msg = $"{this.Name}: Idle lingering timeout has been exceeded. Shipping now";
                     if( this.UseLog4Net )
                        _log.Debug(msg);
                     Debug.WriteLine(msg);
                  }
                  break;
               }
               // parcel was not idle, compute a new delay timespan to recheck if parcel is idle
               timeToDelay = (lastParcelEventTimestamp + _idleShipTimeout + extraTime) - utcNow;
               if( _log.IsDebugEnabled ) {
                  string msg = $"{this.Name}: Parcel delayed for an extra { timeToDelay.TotalMilliseconds} ms";
                  if( this.UseLog4Net )
                     _log.Debug(msg);
                  Debug.WriteLine(msg);
               }
            }
         }
      }

      private async Task ShipParcel(Task<TResultDTO> carryWeightCall, CancellationToken cancelToken) {
         using( await _parcelLock.LockAsync().AsyncTrace().ConfigureAwait(false) ) {
            try {
               await SendParcelToWCFService(_parcel, cancelToken).AsyncTrace()
                                                                 .ConfigureAwait(false);
               await AfterParcelShippedHouseKeeping(_parcel, cancelToken).AsyncTrace()
                                                                         .ConfigureAwait(false);
            } catch( TaskCanceledException ) {
               foreach( ParcelTuple tuple in _parcel ) {
                  tuple.TCS.TrySetCanceled(cancelToken);
               }
            } catch( Exception e ) {
               foreach( ParcelTuple tuple in _parcel ) {
                  tuple.TCS.TrySetException(e);
               }
            } finally {
               _parcel.Clear();
            }
         }
      }

      // Can be overriden if derived class needs to perform housekeeping after the parcel has been shipped
      protected virtual Task AfterParcelShippedHouseKeeping(Parcel parcel, CancellationToken cancelToken) {
         return Task.CompletedTask;
      }

      protected async Task<TResultDTO> ExecuteRequest(TRequestDTO dto, CancellationToken cancelToken) {
         Task<TResultDTO> carryWeightCall = null;
         Task<TResultDTO> freeloaderCall = null;

         using( await _parcelLock.LockAsync().AsyncTrace().ConfigureAwait(false) ) {
            if( _parcel.Count > 0 ) {
               // An active parcel is being assembled, we will piggyback our transmission to the responsible call
               freeloaderCall = AddRequestToParcel(dto, cancelToken);
            } else {
               // this call will own parcel shipment responsibility
               carryWeightCall = AddRequestToParcel(dto, cancelToken);
            }
         }

         if( freeloaderCall != null ) {
            // Wait until responsible call transmits the parcel
            return await freeloaderCall.AsyncTrace()
                                       .ConfigureAwait(false);
         }

         // We are the call that is opening the parcel, we will carry the full weight of the parcel transmission
         await ReadyToShip(cancelToken).AsyncTrace()
                                       .ConfigureAwait(false);
         // no need to await the ongoing ShipParcel task, awaiting the carryWeightCall task will suffice
         Task unobservedTask = ShipParcel(carryWeightCall, cancelToken);
         return await carryWeightCall.AsyncTrace()
                                     .ConfigureAwait(false);
      }

      protected DateTime LastParcelEventTimestamp {
         get {
            lock( _lastParcelEventTimestampLock )
               return _lpets;
         }
         set {
            lock( _lastParcelEventTimestampLock )
               _lpets = value;
         }
      }

      public ParcellerBase(string parcellerName,
                           RobustServiceCaller<TService> service,
                           int maxParcelSize,
                           TimeSpan idleShipTimeout) {

         #region Parameter validation

         if( parcellerName == null )
            throw new ArgumentNullException(nameof(parcellerName));
         if( maxParcelSize <= 0 )
            throw new ArgumentException(Resources.msgInvalidMaxParcelSize, nameof(maxParcelSize));

         #endregion Parameter validation

         this.Name = parcellerName;
         _parcelMaxSize = maxParcelSize;
         _idleShipTimeout = idleShipTimeout;
         _parcelLock = new AsyncLock();
         _lastParcelEventTimestampLock = new object();
         _parcelIsFullEvent = new AsyncManualResetEvent();
         _service = service;
         _parcel = new Parcel();
      }
   }

   // Inherit from this class when the requests have results
   public abstract class RequestParceller<TRequestDTO, TResultDTO, TService> :
                         ParcellerBase<TRequestDTO, TResultDTO, TService> where TService : class {

      public RequestParceller(string parcellerName,
                              RobustServiceCaller<TService> service,
                              int maxParcelSize,
                              TimeSpan idleShipTimeout = default(TimeSpan))
         : base(parcellerName, service, maxParcelSize, idleShipTimeout) {
      }

      public Task<TResultDTO> Request(TRequestDTO dto,
                                      CancellationToken cancelToken = default(CancellationToken)) {
         return ExecuteRequest(dto, cancelToken);
      }
   }

   // Inherit from this class when the requests have no results
   // Derived classes do not need to set the result in the task completion source upon successful execution as this is
   // done automatically by this class
   public abstract class OneWayRequestParceller<TRequestDTO, TService> :
                                  ParcellerBase<TRequestDTO, object, TService> where TService : class {

      protected override Task AfterParcelShippedHouseKeeping(Parcel parcel, CancellationToken cancelToken) {
         if( cancelToken.IsCancellationRequested )
            return Task.CompletedTask;

         foreach( ParcelTuple tuple in parcel )
            tuple.TCS.TrySetResult(null);

         return Task.CompletedTask;
      }

      public OneWayRequestParceller(string parcellerName,
                                    RobustServiceCaller<TService> service,
                                    int maxParcelSize,
                                    TimeSpan idleShipTimeout = default(TimeSpan))
         : base(parcellerName, service, maxParcelSize, idleShipTimeout) {
      }

      public Task Request(TRequestDTO dto,
                          CancellationToken cancelToken = default(CancellationToken)) {
         return ExecuteRequest(dto, cancelToken);
      }
   }
}