import 'dart:async';

import 'package:bloc_concurrency/bloc_concurrency.dart';
import 'package:test/test.dart';

import 'helpers.dart';

void main() {
  group('droppable', () {
    test('forwards synchronous mapper errors with their stack traces',
        () async {
      final expectedError = Exception('mapper failed');
      late StackTrace expectedStackTrace;
      final errors = <Object>[];
      final stackTraces = <StackTrace>[];
      final uncaughtErrors = <Object>[];
      final done = Completer<void>();

      runZonedGuarded(
        () {
          droppable<int>()(
            Stream<int>.value(0),
            (_) {
              try {
                throw expectedError;
              } catch (error, stackTrace) {
                expectedStackTrace = stackTrace;
                rethrow;
              }
            },
          ).listen(
            null,
            onError: (Object error, StackTrace stackTrace) {
              errors.add(error);
              stackTraces.add(stackTrace);
            },
            onDone: done.complete,
          );
        },
        (error, stackTrace) => uncaughtErrors.add(error),
      );

      await done.future;

      expect(errors, [same(expectedError)]);
      expect(stackTraces.single.toString(), expectedStackTrace.toString());
      expect(uncaughtErrors, isEmpty);
    });

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

      droppable<int>()(source.stream, (event) async* {
        started.complete();
        await release.future;
        yield event;
      }).listen(received.add, onDone: outputDone.complete);

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

    test('forwards synchronous mapped subscription errors', () async {
      final reused = StreamController<int>();
      final original = reused.stream.listen((_) {});
      final source = StreamController<int>();
      final done = Completer<void>();
      final errors = <Object>[];
      final stacks = <StackTrace>[];
      final received = <int>[];

      final output = droppable<int>()(source.stream, (event) {
        if (event == 0) return reused.stream;
        return Stream<int>.value(event);
      });
      output.listen(
        received.add,
        onError: (Object error, StackTrace stackTrace) {
          errors.add(error);
          stacks.add(stackTrace);
        },
        onDone: done.complete,
      );

      // listen() throws for an already-listened single-subscription stream.
      source.add(0);
      await tick();
      source.add(1);
      await source.close();
      await done.future.timeout(const Duration(seconds: 1));

      expect(errors, hasLength(1));
      expect(errors.single, isA<StateError>());
      expect(stacks.single, isA<StackTrace>());
      expect(received, [1]);

      await original.cancel();
      await reused.close();
    });

    test('forwards mapper errors and handles later events', () async {
      final source = StreamController<int>();
      final outputDone = Completer<void>();
      final received = <int>[];
      final errors = <Object>[];
      final stackTraces = <StackTrace>[];
      final expectedError = StateError('mapper failed');

      droppable<int>()(source.stream, (event) {
        if (event == 0) throw expectedError;
        return Stream<int>.value(event);
      }).listen(
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

    test('releases the source when cancelled while idle', () async {
      final source = StreamController<int>.broadcast();
      final output = droppable<int>()(
        source.stream,
        (event) => Stream<int>.value(event),
      );
      final subscription = output.listen((_) {});

      expect(source.hasListener, isTrue);
      await subscription.cancel();
      expect(source.hasListener, isFalse);

      await source.close();
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
