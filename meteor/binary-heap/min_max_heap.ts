import { MaxHeap, MaxHeapOptions } from './max_heap';
import { MinHeap } from './min_heap';

// This implementation of Min/Max-Heap is just a subclass of Max-Heap
// with a Min-Heap as an encapsulated property.
//
// Most of the operations are just proxy methods to call the same method on both
// heaps.
//
// This implementation takes 2*N memory but is fairly simple to write and
// understand. And the constant factor of a simple Heap is usually smaller
// compared to other two-way priority queues like Min/Max Heaps
// (http://www.cs.otago.ac.nz/staffpriv/mike/Papers/MinMaxHeaps/MinMaxHeaps.pdf)
// and Interval Heaps
// (http://www.cise.ufl.edu/~sahni/dsaac/enrich/c13/double.htm)
export class MinMaxHeap extends MaxHeap {
    private _minHeap: MinHeap;

    constructor(comparator, options?: MaxHeapOptions) {
        super(comparator, options);
        this._minHeap = new MinHeap(comparator, options);
    }

    set(id: string, value: any) {
        super.set(id, value);
        this._minHeap.set(id, value);
    }

    delete(id: string) {
        super.delete(id);
        this._minHeap.delete(id);
    }

    clear() {
        super.clear();
        this._minHeap.clear();
    }

    setDefault(id: string, def: any) {
        super.setDefault(id, def);
        return this._minHeap.setDefault(id, def);
    }

    clone() {
        const clone = new MinMaxHeap(this._comparator, { initData: this._heap });
        return clone;
    }

    minElementId() {
        return this._minHeap.minElementId();
    }

};