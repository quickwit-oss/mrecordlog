The page directory is a single file that stores a list of pages.

These page themselves are used to store the mrecordlog: a WAL common to all indexes.
Because the mrecordlog contains a several queues, it is possible for  a single queue to
prevent a truncation at the scale of the mrecordlog.

With a page system, a single queue lagging will only prevent the recollection of the
subset of pages holding data for that queue.

The page directory works as follows. All pages have a physical id.
At all point in time, they are all organized in a specific order in which they
are supposed to be read and written in. That order is saved into a header of  the file.

We track the list of pages in use through reference counting. Upon GC, we recompute the right ordering
and written back into the header.

In order to make writing that page list atomic, the header actually consists of two slots.
We track the number of GC operations that have been executed so far: the GC epoch.

We alternatively write the first or the second one based on the parity of the epoch.
When reading, we read both slots and select non-corrupted slot that holds the highest epoch.
That way, if a GC operations is interrupted, it will only corrupt the new slot, and the old
will still be readable.
