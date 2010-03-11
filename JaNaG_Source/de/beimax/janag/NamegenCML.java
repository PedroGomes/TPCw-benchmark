/**
 * $Id: NamegenCML.java 4 2008-12-23 14:52:29Z ronix $
 * File: NamegenCML.java
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
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;


/**
 * @author mkalus
 * NameGenerator command line
 */
public class NamegenCML {
	/**
	 * Namegenerator instance
	 */
	private static Namegenerator ng;

	
	/**
	 * @return number of pattern selected by user
	 * Menu 1 - select pattern
	 */
	public static int menu1() {
		String[] list = ng.getPatterns();

		for (int i = 0; i < list.length; i++)
			System.out.println((i+1) + ") " + list[i]);

		int select = MyUtils.readInt(I18N.geti18nString(Messages.getString("CMLSelectPattern")));

		if (select < 0 || select > list.length)
			System.out.println(I18N.geti18nString(Messages.getString("CMLCorrectNumber")));
		else if (select > 0)
			menu2(list[select-1]);

		return select;
	}

	/**
	 * @param pattern name of pattern to select from
	 * Menu 2 - select gender
	 */
	public static void menu2(String pattern) {
		String[] list = ng.getGenders(pattern);
		int count;

		for (int i = 0; i < list.length; i++)
			System.out.println((i+1) + ") " + list[i]);

		int select = MyUtils.readInt(I18N.geti18nString(Messages.getString("CMLSelectGender")));

		if (select < 0 || select > list.length)
			System.out.println(I18N.geti18nString(Messages.getString("CMLCorrectNumber")));
		if (select == 0) return;

		//ok, now ask for the number of entries
		do {
			count = MyUtils.readInt(I18N.geti18nString(Messages.getString("CMLSelectNumberOfNames")));
			if (count < 0)
				System.out.println(I18N.geti18nString(Messages.getString("CMLCorrectNumber")));
		} while (count < 0);

		if (count == 0) return;

		//generate names and print them
		String[] mynames = ng.getRandomName(pattern, list[select-1], count);

		for (int i = 0; i < count; i++)
			System.out.println(mynames[i]);

		System.out.println();
	}

	/**
	 * @param args arguments
	 */
	public static void main(String[] args) {
		if (args.length == 0) {//Interactive Mode
			//output hint
			ng = new Namegenerator("languages.txt", "semantics.txt");
	
			System.out.println(I18N.geti18nString(Messages.getString("CMLModeHint")));
			
			int m;
			do {
				m = menu1();
			} while (m != 0);
		} else { //Command Line Mode
			//Parse command
			String command = "help";
			String language = "";
			String languagetxt = "languages.txt";
			String semanticstxt = "semantics.txt";
			String[] options = {};

			int idx = 0; //Index counter
			
			if (args[idx].length() == 2) {//Language code
				language = args[idx++].toLowerCase();
			}
			if (idx < args.length && args[idx].toLowerCase().startsWith("files=")) {
				System.out.println("Files!");
				idx++;
			}
			if (idx < args.length) { //catch exceptions
				if (args[idx].toLowerCase().equals("g")) {
					command = args[idx++].toLowerCase();
					
					if (idx == args.length-3) {
						options = new String[3];
						options[0] = args[idx];
						options[1] = args[idx+1];
						options[2] = args[idx+2];
						try {
							int test = Integer.parseInt(args[idx+2]);
							if (test < 1 || test > 1000) throw(new NumberFormatException());
						} catch(NumberFormatException e) {
							command = "help";
						}
					} else if (idx == args.length-2) {
						options = new String[3];
						options[0] = args[idx];
						options[1] = args[idx+1];
						options[2] = "1";
					} else command = "help";
				} else
				if (args[idx].toLowerCase().equals("s")) {
					command = args[idx++].toLowerCase();
	
					if (idx <= args.length-2) command = "help";
					if (idx < args.length) {
						options = new String[1];
						options[0] = args[idx];
					}
				} else
				if (args[idx].toLowerCase().equals("c")) {
					command = args[idx++].toLowerCase();

					if (idx == args.length-5) {
						options = new String[5];
						options[0] = args[idx];
						options[1] = args[idx+1];
						options[2] = args[idx+2];
						options[3] = args[idx+3];
						options[4] = args[idx+4];
						try {
							int test = Integer.parseInt(args[idx+2]);
							if (test < 1 || test > 1000) throw(new NumberFormatException());
							test = Integer.parseInt(args[idx+4]);
							if (test < 1 || test > 65535) throw(new NumberFormatException());
						} catch(NumberFormatException e) {
							command = "help";
						}
					} else if (idx == args.length-4) {
						options = new String[5];
						options[0] = args[idx];
						options[1] = args[idx+1];
						options[2] = args[idx+2];
						options[3] = args[idx+3];
						options[4] = "12022";
						try {
							int test = Integer.parseInt(args[idx+2]);
							if (test < 1 || test > 1000) throw(new NumberFormatException());
						} catch(NumberFormatException e) {
							command = "help";
						}
					} else command = "help";
				}
			}
			_parseCommandLine(command, language, languagetxt, semanticstxt, options);
		}
	}

	/**
	 * @param command command given
	 * @param lang language wanted
	 * @param languagetxt file name of language file
	 * @param semanticstxt file name of semantics file
	 * @param options
	 * private command line parser
	 */
	private static void _parseCommandLine(String command, String lang, String languagetxt, String semanticstxt, String[] options) {
		if (command.equals("help")) {
			I18N.getLangList(lang);
			System.out.println(I18N.geti18nString(Messages.getString("CMLHelp")));
			return;
		}

		//act as client - connect to server
		if (command.equals("c")) {
			//connect to server and send command
			try {
				Socket so = new Socket(options[3], Integer.parseInt(options[4]));
				
				//input stream from server
				BufferedReader receive = new BufferedReader(
						new InputStreamReader(so.getInputStream()));

				//output stream to server
				PrintWriter send = new PrintWriter(
						new OutputStreamWriter(so.getOutputStream()));
				
				//send command to server
				send.println("GET \"" + options[0] + "\" \"" + options[1] + "\" " + options[2]);
				send.flush();
				
				//read answer...
				int c = receive.read();
				while (c != -1) {
					System.out.print("" + (char) c);
					c = receive.read();
				}
				
				so.close();
			} catch (IOException e) {
				System.err.println("Konnte keine Verbindung zum Server aufbauen!");				
			}
			
			return;
		}

		//Create new instance
		ng = new Namegenerator(languagetxt, semanticstxt, lang);

		if (command.equals("g")) {
			//Generate names and print them
			String[] mynames = ng.getRandomName(options[0], options[1], Integer.parseInt(options[2]));

			for (int i = 0; i < mynames.length; i++)
				System.out.println(mynames[i]);

			System.out.println();
			return;
		}

		if (command.equals("s")) {
			String[] list;
			if (options.length == 1) list = ng.getPatterns(options[0]);
			else list = ng.getPatterns();
			
			System.out.println(list.length + " " + 
					I18N.geti18nString(Messages.getString("CMLHits")));

			for (int i = 0; i < list.length; i++) {
				System.out.println("*" + list[i]);
				String[] genders = ng.getGenders(list[i]);
				for (int j = 0; j < genders.length; j++)
					System.out.println("  -" + genders[j]);
			}
		}
	}
}
