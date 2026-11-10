import 'package:bloc/bloc.dart';

/// Process events one at a time by maintaining a queue of added events
/// and processing the events sequentially.
///
/// **Note**: there is no overlap between invocations of the same handler
/// and every event of this type is handled in the order it was received.
/// Handlers registered for other event types can still run concurrently.
/// A burst of incoming events can grow the pending event queue.
EventTransformer<Event> sequential<Event>() {
  return (events, mapper) => events.asyncExpand(mapper);
}
