/**
 * $Id: I18N.java 4 2008-12-23 14:52:29Z ronix $
 * File: I18N.java
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

import java.util.Locale;

/**
 * @author mkalus
 * Internationalizer for strings and data entries...
 */
public class I18N {
	/**
	 * Current language (i18n/l10n)
	 */
	private static int LangIdx;
	/**
	 * List of languages (i18n/l10n)
	 */
	private static String[] LangList; 

	/**
	 * @param line input line
	 * @return part of the line that was internationalized
	 * Reads an internationalized version of a string
	 */
	public static String geti18nString(String line) {
		String elements[] = line.split("\\|"); //$NON-NLS-1$
		String output;
		if (elements.length <= LangIdx) output = elements[0]; //in case none is set
		else {
			output = elements[LangIdx];
			if (output.equals("")) output = elements[0]; //Default Name
		}
		
		return output;
	}

	/**
	 * @param lang language setting
	 * set language to lang (e.g. "en", "de", etc.)
	 */
	public static void getLangList(String lang) {
		//Get Language List
	//	LangList = Messages.getString("Namegenerator.langs").split("\\|"); //$NON-NLS-1$

        /**/ LangList = new String[]{"en"};
		LangIdx = 0;
		//Find Language
		for (int i = 0; i < LangList.length; i++)
			if (LangList[i].equals(lang)) { LangIdx = i+1; break; }
	}

	/**
	 * automatically detect language
	 */
	public static void getLangList() {
        
        String lang  = "en";
        System.out.println("Lang =  "+lang);
		//String lang = Locale.getDefault().toString().substring(0, 2);
		getLangList(lang);
	}
}
