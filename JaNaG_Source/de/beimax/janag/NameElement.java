/**
 * $Id: NameElement.java 3 2008-12-23 13:06:42Z ronix $
 * File: NameElement.java
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
 * A single name element - smallest element of the semantic structure
 */
public class NameElement extends Node {
	/**
	 * first character upper cased?
	 */
	private boolean upperCase;

	/**
	 * @param name
	 * Constructor
	 */
	public NameElement(String name) {
		super(name);
		setUpperCase(false);
	}

	/**
	 * @param name
	 * Constructor
	 */
	public NameElement(String name, boolean upperCase) {
		super(name);
		setUpperCase(upperCase);
	}

	/**
	 * @param name
	 * @param weight
	 * Constructor
	 */
	public NameElement(String name, int weight) {
		super(name, weight);
		setUpperCase(false);
	}

	/**
	 * @return the upperCase
	 */
	public boolean isUpperCase() {
		return upperCase;
	}

	/**
	 * @param ucase the upperCase to set
	 */
	public void setUpperCase(boolean ucase) {
		upperCase = ucase;
	}

}
