/**
 * $Id: List.java 3 2008-12-23 13:06:42Z ronix $
 * File: List.java
 * Package: de.beimax.janag
 * Project: JaNaG
 *
 * Copyright (C) 2008 Maximilian Kalus.  All rights reserved.
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */
package JaNaG_Source.de.beimax.janag;

import java.util.Random;

/**
 * @author mkalus
 *
 */
public class List extends Node {
	/**
	 * Pointer to top node
	 */
	private Node head;
	/**
	 * Pointer to curent node - only needed internaly
	 */
	private Node cursor;
	/**
	 * Number of elements in list
	 */
	private int count;
	/**
	 * weight balance of this list
	 */
	private int balance;
	/**
	 * Pseudo-Random number - static since it is only initialized once per run
	 */
	private static Random r = null;

	/**
	 * @param name
	 * Constructor
	 */
	public List(String name) {
		super(name);
		 _init();
	}

	/**
	 * @param name
	 * @param weight
	 * Constructor
	 */
	public List(String name, int weight) {
		super(name, weight);
		 _init();
	}
	
	/**
	 * Initializes class
	 */
	private void _init() {
		head = null;
		cursor = null;
		count = 0;
		balance = 0;
		if (r == null)
			r = new Random(System.currentTimeMillis());
	}
	
	/**
	 * @return cursor position
	 * Resets cursor to head
	 */
	public Node resetCursor() {
		cursor = head;
		return cursor;
	}
	
	/**
	 * @return node at cursor position
	 * Sets the cursor to the next position of that exists
	 */
	public Node gotoNext() {
		if (cursor != null) //only if next != null
			cursor = cursor.getNext();

		return cursor;
	}

	/**
	 * @return node at cursor position or null if list is empty
	 * Sets the cursor to the last element of the list
	 */
	public Node gotoLast() {
		if (head == null) return null; //if empty list, return null
		if (cursor == null) resetCursor(); //if at end, reset to start

		while (cursor.getNext() != null)
			gotoNext(); //iterate until next == null

		return cursor;
	}

	/**
	 * @return node at cursor position
	 * Returns current node
	 */
	public Node getCurrent() {
		return cursor;
	}

	/**
	 * @param a node to be inserted
	 * Inserts a node into the list
	 */
	public void insertNode(Node a) {
		if (head == null) { //first element inserted
			head = a;
			resetCursor(); //reset cursor
		} else {
			Node last = gotoLast(); //iterate to last element
			last.setNext(a);
			a.setPrev(last);
		}
		count++; //increment counter
		balance += a.getWeight(); //increase weight
	}

	/**
	 * @param a node to be deleted
	 * Deleted a node - this method does not test, if the node is in this list, etc.
	 * Watch out!
	 */
	public void deleteNode(Node a) {
		if (head == a) head = a.getNext(); //if head is deleted
		if (cursor == a) cursor = a.getNext(); //increase cursor to avoid ugly errors

		//set pointers right
		if (a.getPrev() != null) //null-case
			(a.getPrev()).setNext(a.getNext());
		if (a.getNext() != null) //null-case
			(a.getNext()).setPrev(a.getPrev());

		a.setNext(null); //to be sure...
		a.setPrev(null);

		count--; //decrease counter
		balance -= a.getWeight(); //decrease weight
	}

	/**
	 * @return length of list
	 * Returns number of elements in list
	 */
	public int countList() {
		return count;
	}

	/**
	 * @return length of list
	 * Count number of elements again (test method)
	 */
	public int recountList() {
		int counter = 1;

		if (head == null) return 0; //empty list

		resetCursor();
		while (gotoNext() != null) counter++;

		return counter;
	}

	/**
	 * @param num position in list (start with 1)
	 * @return node at position num
	 * Goes to position #num - boundary safe
	 */
	public Node gotoNode(int num) {
		if (num < 1 || head == null) return null;
		if (num > countList()) return null;

		resetCursor();

		for (int i = 1; i < num; i++)
			if (gotoNext() == null) break;

		return cursor;
	}

	/**
	 * @param name name to be searched for
	 * @return node with name name
	 * Searches for a node named name
	 */
	public Node gotoNode(String name) {
		if (head == null) return null;

		resetCursor();

		do {
			if (cursor.getName().equals(name)) return cursor;
		} while (gotoNext() != null);

		return null;
	}

	/**
	 * @return random node
	 * Returns a random node without considering weights
	 */
	public Node getRandom() {
		return gotoNode(r.nextInt(countList())+1);
	}

	/**
	 * @return random node
	 * Returns a random node with considering weights
	 */
	public Node getWeightedRandom() {
		int c = r.nextInt(balance)+1; //random number based on weight

		resetCursor();
		while (c > cursor.getWeight()) { //iterate through elements to look for right node
			c -= cursor.getWeight(); //decrease weight until it is small than...
			if (gotoNext() == null) return null; //the current node's
		}

		return cursor;
	}
}
