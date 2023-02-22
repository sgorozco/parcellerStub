# parcellerStub
Just a stub for quickly sharing some 
Not compilable, as there are plenty of dependencies that I'm not ready to share, however the parcelling logic is visible and may serve as inspiration ;)


The RequestParceller related classes provide basic WCF parcelling request services.
Use these classes to reduce WCF chattiness by bundling multiple, related WCF service calls from multiple threads into a single call.

IMPORTANT:
By default, logging to log4net is disabled. To enable logging set the UseLog4Net property to true. The reason for
this default configuration is that a common use for this class is parcelling log events that will be sent to a
remote viewer via WCF. If we log in this class, we will trigger an infinite cascade of logging events and will
degrade system performance greatly. If the parceller is not forwarding log events, it is safe to set this property
to True

To the client code, the call appears to be executed independently (the parcelling is fully hidden from client
code).
The parcelling policy is bounded by two conditions:  a maximum parcel size or a parcel-idle timeout. Whenever any
of the two conditions are reached, the parcel is sent to the WCF service as a single call

The provided WCF service inteface must expose a WCF operation that can receive a collection of multiple DTO
(data-transmission-objects) instances (one for each parcelled request).

The classes are abstract. A concrete implementation of the class must be supplied, overriding the
SendParcelToWCFService() method.  The SendParcelToWCFService() method must call the WCF service by means of the
 _service member, passing the DTO objects available in the parcelled tuples.  After the WCF call is completed,
the overriden SendParcelToWCFService() method must set the results of the WCF call into the TaskCompletionSource
(TCO tuple member) objects in the corresponding parcelled tuples.  This action will resume all the multiple,
individual calls that are awaiting for their requests to be fulfilled.

Usage Example:

```C#
public class AudioHashParceller : ParcellerBase<AudioHashDTO, AudioHashResultDTO, IAudioHashService> {
   protected async override Task SendParcelToWCFService(Parcel parcel, CancellationToken cancelToken) {
         // obtain a list of the DTO objects stored in the parcel tuples
         List<AudioHashDTO> hashes = parcel.Select(item => item.DTO).ToList();
         List<AudioHashResultDTO> results = await _service.ProcessMultipleHashes(hashes)
                                                          .ConfigureAwait(false);
         if( results.Count != hashes.Count )
            throw new Exception("expecting the same amount of results as inputs");
         for( int i = 0; i < results.Count; i++ )
            parcel[i].TCS.SetResult(results[i]);  // resume the method awaiting this result
      }
   }
   
    ...
   
   // Create a parceller that will ship (transmit) whenever 3 items are parcelled by different threads, or 1
   // second has elapsed from the last item bundled in the active parcel
   
   var audioHashParceller = new AudioHashParceller(wcfSevice,
                                                   maxParcelSize: 3,
                                                  idleShipTimeout: TimeSpan.FromSeconds(1.0));
   // Invoke the service
   AudioHashResultDTO result1 = await audioHashParceller.Request(GetAudioHashDTO());
```
