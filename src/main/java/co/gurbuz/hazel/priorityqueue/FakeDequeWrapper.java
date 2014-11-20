package co.gurbuz.hazel.priorityqueue;

import java.util.Deque;
import java.util.Iterator;
import java.util.concurrent.PriorityBlockingQueue;

public class FakeDequeWrapper<E> extends PriorityBlockingQueue<E> implements Deque<E> {
    @Override
    public void addFirst(E e) {
        add(e);
    }

    @Override
    public void addLast(E e) {
        add(e);
    }

    @Override
    public boolean offerFirst(E e) {
        return offer(e);
    }

    @Override
    public boolean offerLast(E e) {
        return offer(e);
    }

    @Override
    public E removeFirst() {
        return remove();
    }

    @Override
    public E removeLast() {
        return remove();
    }

    @Override
    public E pollFirst() {
        return poll();
    }

    @Override
    public E pollLast() {
        return poll();
    }

    @Override
    public E getFirst() {
        return peek();
    }

    @Override
    public E getLast() {
        return peek();
    }

    @Override
    public E peekFirst() {
        return peek();
    }

    @Override
    public E peekLast() {
        return peek();
    }

    @Override
    public boolean removeFirstOccurrence(Object o) {
        return false;
    }

    @Override
    public boolean removeLastOccurrence(Object o) {
        return false;
    }

    @Override
    public void push(E e) {
        addFirst(e);
    }

    @Override
    public E pop() {
        return removeFirst();
    }

    @Override
    public Iterator<E> descendingIterator() {
        return iterator();
    }
}