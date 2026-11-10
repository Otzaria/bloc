// Run from packages/bloc_concurrency:
// dart run benchmark/droppable_benchmark.dart
//
// This benchmark reports accepted handlers and source cancellation latency.
// It does not use wall-clock thresholds as pass/fail assertions.
import 'dart:async';
import 'dart:io';

import 'package:bloc_concurrency/bloc_concurrency.dart';

Future<void> main() async {
  await _measureBurst(100000);
  await _measureCancellation();
}

Future<void> _measureBurst(int count) async {
  final source = StreamController<int>(sync: true);
  final started = Completer<void>();
  final release = Completer<void>();
  final done = Completer<void>();
  var mappedCalls = 0;

  droppable<int>()(source.stream, (event) async* {
    mappedCalls++;
    started.complete();
    await release.future;
    yield event;
  }).listen((_) {}, onDone: done.complete);

  source.add(0);
  await started.future;

  final watch = Stopwatch()..start();
  for (var i = 1; i < count; i++) {
    source.add(i);
  }
  watch.stop();

  release.complete();
  await source.close();
  await done.future;

  stdout.writeln(
    'burst: events=$count, mapped=$mappedCalls, '
    'dispatch_us=${watch.elapsedMicroseconds}',
  );
}

Future<void> _measureCancellation() async {
  final sourceCancelled = Completer<void>();
  final releaseMappedCancel = Completer<void>();
  final source = StreamController<int>(
    onCancel: sourceCancelled.complete,
  );
  final mapped = StreamController<int>(
    onCancel: () => releaseMappedCancel.future,
  );
  final output = droppable<int>()(source.stream, (_) => mapped.stream);
  final subscription = output.listen((_) {});

  source.add(0);
  await Future<void>.delayed(Duration.zero);

  final releaseTimer = Timer(
    const Duration(milliseconds: 100),
    releaseMappedCancel.complete,
  );
  final watch = Stopwatch()..start();
  final cancellation = subscription.cancel();

  await sourceCancelled.future;
  watch.stop();

  await cancellation;
  releaseTimer.cancel();
  await source.close();
  await mapped.close();

  stdout.writeln(
    'source_cancel_us=${watch.elapsedMicroseconds} '
    '(mapped cleanup takes about 100ms)',
  );
}
