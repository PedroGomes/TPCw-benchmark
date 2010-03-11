/**
 * $Id: Node.java 3 2008-12-23 13:06:42Z ronix $
 * File: Node.java
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

/**
 * @author mkalus
 * A weighted linked list.
 */
public class Node extends NamedObject {
	/**
	 * Pointer to next element
	 */
	private Node next;
	/**
	 * Pointer to previous element
	 */
	private Node prev;
	/**
	 * Weight of the node - default is 10
	 */
	private int weight;

	/**
	 * @param name
	 * Constructor
	 */
	public Node(String name) {
		super(name);
		_init(10);
	}

	/**
	 * @param name
	 * Constructor
	 */
	public Node(String name, int weight) {
		super(name);
		_init(weight);
	}

	private void _init(int myweight) {
		next = null;
		prev = null;
		weight = myweight;
	}

	/**
	 * @return the next node
	 */
	public Node getNext() {
		return next;
	}

	/**
	 * @param next the next node to set
	 */
	public void setNext(Node next) {
		this.next = next;
	}

	/**
	 * @return the previous node
	 */
	public Node getPrev() {
		return prev;
	}

	/**
	 * @param prev the previous node to set
	 */
	public void setPrev(Node prev) {
		this.prev = prev;
	}

	/**
	 * @return the weight
	 */
	public int getWeight() {
		return weight;
	}

	/**
	 * @param weight the weight to set
	 */
	public void setWeight(int weight) {
		this.weight = weight;
	}
}
