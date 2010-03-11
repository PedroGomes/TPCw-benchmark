/**
 * $Id: Namegenerator.java 8 2008-12-23 16:57:17Z ronix $
 * File: Namegenerator.java
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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * @author mkalus
 * The name generator - core of the program
 */
public class Namegenerator {
	/**
	 * List of language groups - keeps linguistic structure
	 */
	private List langGroups;
	/**
	 * List of name patterns - keeps semantic structure
	 */
	private List namePatterns;
	
	/**
	 * @param languageFile filename of language file
	 * @param semanticsFile filename of semantics file
	 * Constructor
	 */
	public Namegenerator(String languageFile, String semanticsFile) {
		I18N.getLangList(); //set language
		readSemantics(semanticsFile);
		readLanguage(languageFile);
	}
	
	/**
	 * @param languageFile filename of language file
	 * @param semanticsFile filename of semantics file
	 * @param lang locale entry, e.g. "de", "nl", ...
	 * Constructor
	 */
	public Namegenerator(String languageFile, String semanticsFile, String lang) {
		I18N.getLangList(lang); //set  specific language
		readSemantics(semanticsFile);
		readLanguage(languageFile);
	}

	/**
	 * @param semanticsFile filename of semantics file
	 * Reads the semantic file and parses it into the semantic structure
	 */
	public void readSemantics(String semanticsFile) {
		String line;
		NamePattern pattern = null;
		NameGenderGroup group = null;
		NameGroup namegroup = null;
		int myweight = 10;
		
		namePatterns = new List("[NamePatterns]"); //Instantiate Object
		
		//open file
		File file = new File(semanticsFile);
		//check file existence
		//if(!file.exists()) {
		//	System.err.println(Messages.getString("Namegenerator.FileDoesNotExist")); //$NON-NLS-1$
		//	System.exit(0);
		//}

		try { //read file by line
			//either get the file as resource or as file stream
			InputStream fIS = getClass().getResourceAsStream(file.getName());
			if (fIS == null) fIS = new FileInputStream(file);
			//buffered input as UTF8
			BufferedReader lNR = new BufferedReader(new InputStreamReader(fIS, "UTF8"));

			//traverse the file
			while ((line = lNR.readLine()) != null) {
				
				if (line.trim().equals("[End]")) break; //if end is reached - break while
				
				//ignore empty lines and comments
				if (!line.startsWith("##") && !line.trim().equals("")) {
					if (pattern == null && (line.charAt(0) != '['))
						throw new IOException(I18N.geti18nString(Messages.getString("Namegenerator.SemanticsFileSyntaxError")) + line); //$NON-NLS-1$
					else if (pattern != null && group == null && (line.charAt(0) != ':'))
						throw new IOException(I18N.geti18nString(Messages.getString("Namegenerator.NoGenderSelected"))); //$NON-NLS-1$
					if (line.charAt(0) == '[') {
						line = line.substring(1,line.length()-1);
						pattern = new NamePattern(I18N.geti18nString(line)); //new group

						namePatterns.insertNode(pattern);
					} else if (line.charAt(0) == ':') {
						line = line.substring(1);
						group = new NameGenderGroup(I18N.geti18nString(line)); //new group

						pattern.insertNode(group);
					} else {
						String[] part = MyUtils.split(line, ":");
						if (part.length != 3)
							throw new IOException(I18N.geti18nString(Messages.getString("Namegenerator.WrongNameStart")) + line + I18N.geti18nString(Messages.getString("Namegenerator.WrongNameEnd"))); //$NON-NLS-1$ //$NON-NLS-2$

						if (part[1].equals(""))
							myweight = 10;
						else
							try {
								myweight = Integer.parseInt(part[1]);
							} catch (NumberFormatException e) {
								e.printStackTrace();
							}

						namegroup = new NameGroup(part[0], myweight); //new element

						group.insertNode(namegroup);

						String[] subpart = MyUtils.split(part[2], ",");
						for (int i = 0; i < subpart.length; i++) {
							boolean uc;
							if (subpart[i].charAt(0) == '^') {
								subpart[i] = subpart[i].substring(1);
								uc = true;
							} else uc = false;
							namegroup.insertNode(new NameElement(I18N.geti18nString(subpart[i]), uc));
						}
					}
				}
			}

			fIS.close();
			fIS = null;
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param languageFile filename of language file
	 * Reads the language file and parses it into the linguistic structure
	 */
	public void readLanguage(String languageFile) {
		String line;
		LangGroup group = null;
		LangElement elem = null;

		langGroups = new List("[LangGroups]"); //Instantiate Object

		//open file
		File file = new File(languageFile);
		//check file existence
		//if(!file.exists()) {
		//	System.err.println(Messages.getString("Namegenerator.FileDoesNotExist")); //$NON-NLS-1$
		//	System.exit(0);
		//}

		try { //read file by line
			//either get the file as resource or as file stream
			InputStream fIS = getClass().getResourceAsStream(file.getName());
			if (fIS == null) fIS = new FileInputStream(file);
			//buffered input as UTF8
			BufferedReader lNR = new BufferedReader(new InputStreamReader(fIS, "UTF8"));

			//traverse the file
			while ((line = lNR.readLine()) != null) {

				if (line.trim().equals("[End]")) break; //if end is reached - break while
				
				//ignore empty lines and comments
				if (!line.startsWith("##") && !line.trim().equals("")) {
					if (group == null && line.charAt(0) != '[')
						throw new IOException(I18N.geti18nString(Messages.getString("Namegenerator.LanguageFileSyntaxError")) + line); //$NON-NLS-1$
					if (line.charAt(0) == '[') {
						line = line.substring(1,line.length()-1);
						group = new LangGroup(I18N.geti18nString(line)); //new group

						langGroups.insertNode(group);
					} else {
						if (line.charAt(0) == '$')
							line = line.substring(1);
						else line = line.trim();
						elem = new LangElement(I18N.geti18nString(line)); //new element

						group.insertNode(elem);
					}
				}
			}

			fIS.close();
			fIS = null;
		} catch(FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * @param pattern name of pattern
	 * @param gender name of gender in that pattern
	 * @param count number of random names to generate
	 * @return array of random names
	 * Creates an array of randomly generated names based on pattern/gender
	 */
	public String[] getRandomName(String pattern, String gender, int count) {
		NameGroup g;
		String s, temp;
		LangGroup lg;
		LangElement e;
		NameElement ne;
		char ch;

		if (count < 1) {
			String[] back = new String[1];
			back[0] = I18N.geti18nString(Messages.getString("Namegenerator.CountLessThanOne")); //$NON-NLS-1$
			return back;
		}
		String[] back = new String[count];

		//Test pattern and gender...
		NamePattern p = (NamePattern) namePatterns.gotoNode(pattern);

		if (p == null) {
			back[0] = I18N.geti18nString(Messages.getString("Namegenerator.NoPatternFound")) + pattern; //$NON-NLS-1$
			return back;
		}

		NameGenderGroup gg = (NameGenderGroup) p.gotoNode(gender);
		
		if (gg == null) {
			back[0] = I18N.geti18nString(Messages.getString("Namegenerator.NoGenderFound")) + gender; //$NON-NLS-1$
			return back;
		}

		//everything ok - generate list!
		for (int i = 0; i < count; i++) {
			s = new String();
			g = (NameGroup) gg.getWeightedRandom();
			ne = (NameElement) g.resetCursor();
			for (int j = 0; j < g.countList(); j++) { //join...
				lg = (LangGroup) langGroups.gotoNode(ne.getName());
				if (lg == null) s += I18N.geti18nString(Messages.getString("Namegenerator.ErrorGeneric")); //$NON-NLS-1$
				else {
					e = (LangElement) lg.getRandom();
	                temp = e.getName();
	                if (ne.isUpperCase()) { //Upper case set?
						ch = temp.charAt(0);
						if (Character.isLetter(ch))
						temp = Character.toUpperCase(ch) + temp.substring(1);
	                }
	                s += temp;
				}
				ne = (NameElement) g.gotoNext();
			}
			back[i] = s;
		}

		return back;
	}

	/**
	 * @param pattern name of pattern
	 * @param gender name of gender in that pattern
	 * @return String with one random name
	 * Creates one randomly generated name based on pattern/gender
	 */
	public String getRandomName(String pattern, String gender) {
		return getRandomName(pattern, gender, 1)[0]; //return first element
	}

	/**
	 * @return list of all possible patterns
	 * Returns an array of all possible patterns
	 */
	public String[] getPatterns() {
		Node n;

		String[] back = new String[namePatterns.countList()];

		n = namePatterns.resetCursor();
		for (int i = 0; i < namePatterns.countList(); i++) {
			back[i] = n.getName();
			n = namePatterns.gotoNext();
		}

		return back;
	}

	/**
	 * @param search string (pattern name has to start with this)
	 * @return list of all patterns matching search
	 * Searches patterns for a string
	 */
	public String[] getPatterns(String search) {
		Node n;
		int max = 0, counter = 0;
		String mysearch = search.toLowerCase();

		//Count first
		n = namePatterns.resetCursor();
		for (int i = 0; i < namePatterns.countList(); i++) {
			if (n.getName().toLowerCase().startsWith(mysearch)) max++;
			n = namePatterns.gotoNext();
		}

		String[] back = new String[max];

		n = namePatterns.resetCursor();
		for (int i = 0; i < namePatterns.countList(); i++) {
			if (n.getName().toLowerCase().startsWith(mysearch))
				back[counter++] = n.getName();
			n = namePatterns.gotoNext();
		}

		return back;
	}

	/**
	 * @param pattern to be examined
	 * @return all possible genders belonging to a pattern
	 * Returns an array of genders of pattern pattern.
	 */
	public String[] getGenders(String pattern) {
		Node n;

		//search pattern
		NamePattern p = (NamePattern) namePatterns.gotoNode(pattern);

		if (p == null) return null; //not found -> return null

		String[] back = new String[p.countList()];

		n = p.resetCursor();
		for (int i = 0; i < p.countList(); i++) {
			back[i] = n.getName();
			n = p.gotoNext();
		}

		return back;
	}
}
