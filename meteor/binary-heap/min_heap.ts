import { MaxHeap, MaxHeapOptions } from './max_heap';

export class MinHeap extends MaxHeap {
    constructor(comparator, options?: MaxHeapOptions) {
        super((a, b) => -comparator(a, b), options);
    }

    maxElementId() {
        throw new Error("Cannot call maxElementId on MinHeap");
    }

    minElementId() {
        return super.maxElementId();
    }
};