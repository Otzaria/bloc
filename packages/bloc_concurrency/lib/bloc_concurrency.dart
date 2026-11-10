/// Custom event transformers inspired by ember concurrency.
/// Built to be used with the [bloc](https://pub.dev/packages/bloc) state management package.
///
/// Each transformer applies only to the `on<Event>` handler it is passed to.
/// Handlers registered for other event types are not serialized with,
/// dropped by, or cancelled by it.
///
/// Get started at [bloclibrary.dev](https://bloclibrary.dev) 🚀
library bloc_concurrency;

export 'src/concurrent.dart';
export 'src/droppable.dart';
export 'src/restartable.dart';
export 'src/sequential.dart';
