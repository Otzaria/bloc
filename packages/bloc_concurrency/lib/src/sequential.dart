import 'package:bloc/bloc.dart';

/// Process events one at a time by maintaining a queue of added events
/// and processing the events sequentially.
///
/// **Note**: there is no handler overlap within this event registration,
/// and every event of this type is handled in arrival order. Other event
/// types registered with `on<OtherEvent>` can still run concurrently.
/// A burst of incoming events can grow the pending event queue.
EventTransformer<Event> sequential<Event>() {
  return (events, mapper) => events.asyncExpand(mapper);
}
