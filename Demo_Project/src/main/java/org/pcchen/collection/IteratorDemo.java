package org.pcchen.collection;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author ceek
 * @date 2021/3/6 17:03
 */
public class IteratorDemo {
    public static void main(String[] args) {
        List<String> stringList = new ArrayList<String>();
        stringList.add("a");
        stringList.add("b");

        Iterator<String> stringIterator = stringList.iterator();

        while (stringIterator.hasNext()) {
            System.out.println(stringIterator.next());
        }

        System.out.println(stringIterator);
    }
}