import 'rxjs';
import { map } from 'rxjs/map';
import { pipe } from 'rxjs/pipe';
import { scan } from 'rxjs/scan';
import { switchMap } from 'rxjs/switch-map';
import { timeout } from 'rxjs/timeout';
import { timer } from 'rxjs/timer';

const target = globalThis as typeof globalThis & {
  __rxjsExtensionKernelPilot?: { map: symbol; pipe: symbol; scan: symbol; switchMap: symbol; timeout: symbol; timer: symbol };
};

target.__rxjsExtensionKernelPilot = { map, pipe, scan, switchMap, timeout, timer };

// Exercise native Web IDL observer callback invocation with subscriber instances.
import { Subject } from 'rxjs/subject';
import { AsyncSubject } from 'rxjs/async-subject';
import { connectable } from 'rxjs/connectable';
import { connect } from 'rxjs/connect';
import { shareReplay } from 'rxjs/share-replay';
import { share } from 'rxjs/share';

(globalThis as any).__rxjsNativeSubscriberForwarding = () => {
  const subject = new Subject<number>();
  const values: number[] = [];
  const controller = new AbortController();
  subject.asObservable().subscribe((value) => values.push(value), { signal: controller.signal });
  subject.next(1);
  controller.abort();
  subject.next(2);
  if (values.join(',') !== '1') throw new Error(`Subject.asObservable forwarding failed: ${values}`);

  let upstream: Subscriber<number> | undefined;
  let tornDown = false;
  const source = new Observable<number>((subscriber) => {
    upstream = subscriber;
    subscriber.addTeardown(() => {
      tornDown = true;
    });
  });
  const sharedValues: number[] = [];
  const sharedController = new AbortController();
  source[share]().subscribe((value) => sharedValues.push(value), { signal: sharedController.signal });
  upstream?.next(3);
  sharedController.abort();
  if (sharedValues.join(',') !== '3' || !tornDown) throw new Error(`share forwarding or cancellation failed: ${sharedValues}`);

  const asyncSubject = new AsyncSubject<number>();
  const asyncValues: number[] = [];
  let asyncDone = false;
  asyncSubject.asObservable().subscribe({
    next: (value) => asyncValues.push(value),
    complete: () => {
      asyncDone = true;
    },
  });
  asyncSubject.next(4);
  asyncSubject.complete();
  if (asyncValues.join(',') !== '4' || !asyncDone) throw new Error(`AsyncSubject forwarding failed: ${asyncValues}`);

  const connectedValues: number[] = [];
  let connectedDone = false;
  const connected = connectable(
    new Observable<number>((subscriber) => {
      subscriber.next(5);
      subscriber.complete();
    })
  );
  connected.subscribe({
    next: (value) => connectedValues.push(value),
    complete: () => {
      connectedDone = true;
    },
  });
  connected.connect();
  if (connectedValues.join(',') !== '5' || !connectedDone) throw new Error(`connectable forwarding failed: ${connectedValues}`);

  const replayValues: number[] = [];
  const replayed = new Observable<number>((subscriber) => {
    subscriber.next(6);
    subscriber.complete();
  })[shareReplay](1);
  replayed.subscribe((value) => replayValues.push(value));
  replayed.subscribe((value) => replayValues.push(value));
  if (replayValues.join(',') !== '6,6') throw new Error(`shareReplay forwarding failed: ${replayValues}`);

  const error = new Error('forwarded error');
  let seenError: unknown;
  new Observable<number>((subscriber) => subscriber.error(error))
    [connect]((shared) => shared)
    .subscribe({
      error: (value) => {
        seenError = value;
      },
    });
  if (seenError !== error) throw new Error('connect did not forward errors');
};
