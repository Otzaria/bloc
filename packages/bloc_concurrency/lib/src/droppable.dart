import 'dart:async';

import 'package:bloc/bloc.dart';

/// Process only one event and ignore (drop) any new events
/// until the current event is done.
///
/// **Note**: dropped events never trigger the event handler.
EventTransformer<Event> droppable<Event>() {
  return (events, mapper) {
    return events.transform(_ExhaustMapStreamTransformer(mapper));
  };
}

class _ExhaustMapStreamTransformer<T> extends StreamTransformerBase<T, T> {
  _ExhaustMapStreamTransformer(this.mapper);

  final EventMapper<T> mapper;

  @override
  Stream<T> bind(Stream<T> stream) {
    late StreamSubscription<T> subscription;
    StreamSubscription<T>? mappedSubscription;
    var isSourceDone = false;

    final controller = StreamController<T>(
      onCancel: () {
        // A slow mapped cancellation must not delay releasing the source.
        final mappedCancellation = mappedSubscription?.cancel();
        final sourceCancellation = subscription.cancel();
        if (mappedCancellation == null) return sourceCancellation;
        return Future.wait<void>(
          [mappedCancellation, sourceCancellation],
        ).then<void>((_) {});
      },
      sync: true,
    );

    // The source may finish before the active mapped stream has completed.
    void maybeClose() {
      if (isSourceDone && mappedSubscription == null) {
        controller.close();
      }
    }

    subscription = stream.listen(
      (data) {
        if (mappedSubscription != null) return;
        final Stream<T> mappedStream;

        try {
          mappedStream = mapper(data);
        } catch (error, stackTrace) {
          // A synchronous mapper failure is an output stream error, not an
          // uncaught zone error. Later source events remain eligible.
          controller.addError(error, stackTrace);
          return;
        }
        mappedSubscription = mappedStream.listen(
          controller.add,
          onError: controller.addError,
          onDone: () {
            mappedSubscription = null;
            maybeClose();
          },
        );
      },
      onError: controller.addError,
      onDone: () {
        isSourceDone = true;
        maybeClose();
      },
    );

    return controller.stream;
  }
}
