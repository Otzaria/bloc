import 'dart:async';

import 'package:bloc_concurrency/bloc_concurrency.dart';
import 'package:test/test.dart';

import 'helpers.dart';

void main() {
  group('droppable', () {
    test('processes only the current event and ignores remaining', () async {
      final states = <int>[];
      final bloc = CounterBloc(droppable())
        ..stream.listen(states.add)
        ..add(Increment())
        ..add(Increment())
        ..add(Increment());

      await tick();

      expect(bloc.onCalls, equals([Increment()]));

      await wait();

      expect(bloc.onEmitCalls, equals([Increment()]));
      expect(states, equals([1]));

      bloc
        ..add(Increment())
        ..add(Increment())
        ..add(Increment());

      await tick();

      expect(
        bloc.onCalls,
        equals([Increment(), Increment()]),
      );

      await wait();

      expect(
        bloc.onEmitCalls,
        equals([Increment(), Increment()]),
      );
      expect(states, equals([1, 2]));

      bloc
        ..add(Increment())
        ..add(Increment())
        ..add(Increment());

      await tick();

      expect(
        bloc.onCalls,
        equals([Increment(), Increment(), Increment()]),
      );

      await wait();

      expect(
        bloc.onEmitCalls,
        equals([Increment(), Increment(), Increment()]),
      );
      expect(states, equals([1, 2, 3]));

      await bloc.close();

      expect(
        bloc.onCalls,
        equals([Increment(), Increment(), Increment()]),
      );

      expect(
        bloc.onEmitCalls,
        equals([Increment(), Increment(), Increment()]),
      );

      expect(states, equals([1, 2, 3]));
    });

    test('cancels the mapped subscription when it is active.', () async {
      final states = <int>[];
      final controller = StreamController<int>.broadcast();
      final stream = droppable<int>()(controller.stream, (x) async* {
        await wait();
        yield x;
      });

      final subscription = stream.listen(states.add);

      controller.add(0);

      await wait();

      expect(states, isEmpty);
      expect(controller.hasListener, isTrue);

      await subscription.cancel();

      expect(states, isEmpty);
      expect(controller.hasListener, isFalse);

      await controller.close();

      expect(states, isEmpty);
    });

    test('closes the output after an in-flight mapped event', () async {
      final source = StreamController<int>();
      final started = Completer<void>();
      final release = Completer<void>();
      final outputDone = Completer<void>();
      final received = <int>[];

      final stream = droppable<int>()(source.stream, (event) async* {
        started.complete();
        await release.future;
        yield event;
      });
      stream.listen(received.add, onDone: outputDone.complete);

      source.add(7);
      await started.future;
      await source.close();

      expect(outputDone.isCompleted, isFalse);

      release.complete();
      await outputDone.future.timeout(const Duration(seconds: 1));

      expect(received, [7]);
    });

    test('closes the output immediately when no event is in flight', () async {
      final output = droppable<int>()(
        const Stream<int>.empty(),
        (event) => Stream<int>.value(event),
      );

      expect(await output.toList(), isEmpty);
    });

    test('forwards mapper errors and handles later events', () async {
      final source = StreamController<int>();
      final outputDone = Completer<void>();
      final received = <int>[];
      final errors = <Object>[];
      final stackTraces = <StackTrace>[];
      final expectedError = StateError('mapper failed');

      final output = droppable<int>()(source.stream, (event) {
        if (event == 0) throw expectedError;
        return Stream<int>.value(event);
      });

      output.listen(
        received.add,
        onError: (Object error, StackTrace stackTrace) {
          errors.add(error);
          stackTraces.add(stackTrace);
        },
        onDone: outputDone.complete,
      );

      source.add(0);
      await tick();
      source.add(1);
      await source.close();
      await outputDone.future.timeout(const Duration(seconds: 1));

      expect(errors, [same(expectedError)]);
      expect(stackTraces.single, isA<StackTrace>());
      expect(received, [1]);
    });

    test('pauses both source and mapped streams to avoid buffering', () async {
      var sourcePauses = 0;
      var sourceResumes = 0;
      var mappedPauses = 0;
      var mappedResumes = 0;
      final source = StreamController<int>(
        onPause: () => sourcePauses++,
        onResume: () => sourceResumes++,
      );
      final mapped = StreamController<int>(
        onPause: () => mappedPauses++,
        onResume: () => mappedResumes++,
      );
      final received = <int>[];
      final done = Completer<void>();
      final output = droppable<int>()(source.stream, (_) => mapped.stream);
      final subscription = output.listen(received.add, onDone: done.complete);

      source.add(1);
      await tick();

      subscription.pause();
      expect(sourcePauses, 1);
      expect(mappedPauses, 1);

      mapped.add(9);
      source.add(2);
      await tick();
      expect(received, isEmpty);

      subscription.resume();
      await tick();

      expect(sourceResumes, 1);
      expect(mappedResumes, 1);
      expect(received, [9]);

      await mapped.close();
      await source.close();
      await done.future.timeout(const Duration(seconds: 1));
    });

    test('cancels source promptly if mapped cancellation stalls', () async {
      final mappedCancelStarted = Completer<void>();
      final mappedCancelRelease = Completer<void>();
      final sourceCancelled = Completer<void>();
      final source = StreamController<int>(
        onCancel: sourceCancelled.complete,
      );
      final mapped = StreamController<int>(
        onCancel: () {
          mappedCancelStarted.complete();
          return mappedCancelRelease.future;
        },
      );

      final output = droppable<int>()(source.stream, (_) => mapped.stream);
      final subscription = output.listen((_) {});
      source.add(1);
      await tick();

      final cancelling = subscription.cancel();
      await mappedCancelStarted.future.timeout(const Duration(seconds: 1));
      try {
        await sourceCancelled.future.timeout(const Duration(seconds: 1));
      } finally {
        mappedCancelRelease.complete();
      }
      await cancelling.timeout(const Duration(seconds: 1));
      await source.close();
      await mapped.close();
    });

    test('drops a large burst without starting extra handlers', () async {
      final source = StreamController<int>(sync: true);
      final started = Completer<void>();
      final release = Completer<void>();
      final done = Completer<void>();
      var mapperCalls = 0;

      final output = droppable<int>()(source.stream, (event) async* {
        mapperCalls++;
        started.complete();
        await release.future;
        yield event;
      });
      final received = <int>[];
      output.listen(received.add, onDone: done.complete);

      source.add(0);
      await started.future;
      for (var i = 1; i <= 10000; i++) {
        source.add(i);
      }
      expect(mapperCalls, 1);

      release.complete();
      await source.close();
      await done.future.timeout(const Duration(seconds: 1));
      expect(received, [0]);
    });
  });
}
