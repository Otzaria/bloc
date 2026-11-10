/// Custom event transformers inspired by ember concurrency.
/// Built to be used with the [bloc](https://pub.dev/packages/bloc) state management package.
///
/// Event transformers apply to each individual `on<Event>` registration.
/// Different registered event types are not globally serialized or cancelled.
///
/// Get started at [bloclibrary.dev](https://bloclibrary.dev) 🚀
library bloc_concurrency;

export 'src/concurrent.dart';
export 'src/droppable.dart';
export 'src/restartable.dart';
export 'src/sequential.dart';
