import 'package:bloc/bloc.dart';
import 'package:stream_transform/stream_transform.dart';

/// Process events concurrently.
///
/// **Note**: there may be event handler overlap and state changes will occur
/// as soon as they are emitted. There is no built-in limit on in-flight
/// event handlers, which can increase memory and I/O use under heavy load.
/// States may be emitted in an order that does not match the order in which
/// the corresponding events were added.
EventTransformer<Event> concurrent<Event>() {
  return (events, mapper) => events.concurrentAsyncExpand(mapper);
}
