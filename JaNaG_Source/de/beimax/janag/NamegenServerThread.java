/**
 * $Id: NamegenServerThread.java 8 2008-12-23 16:57:17Z ronix $
 * File: NamegenServerThread.java
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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.net.Socket;

/**
 * @author mkalus
 * Working thread of ng server
 */
public class NamegenServerThread extends Thread {
	/**
	 * Socket of worker
	 */
	private Socket so;

	/**
	 * static reference to name generator - used by all threads
	 */
	private static Namegenerator ng = new Namegenerator("languages.txt", "semantics.txt");
	
	/**
	 * @param cs socket of worker
	 * Constructor
	 */
	public NamegenServerThread(Socket cs) {
		so = cs;
	}
	
	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {
		String command = "";

		System.out.println(so.getInetAddress().getHostAddress() + ": Open.");
		
		try {
			//Input stream from client
			BufferedReader receive = new BufferedReader(
					new InputStreamReader(so.getInputStream()));
	
			//Output stream to client
			BufferedWriter send = new BufferedWriter(
					new OutputStreamWriter(so.getOutputStream()));
		
			try {
				command = receive.readLine();
				System.out.println(so.getInetAddress().getHostAddress() + ": Command: " + command);
				
				//parse command
				String[] echo = ParseCommand(command);
				
				for (int i = 0; i < echo.length; i++) {
					System.out.println(so.getInetAddress().getHostAddress() + ": Answer: " + echo[i]);
					send.write(echo[i] + "\n");
				}
			} catch (Exception e) {
				new PrintWriter(send).println("Error!");
				System.err.println("Error in command: " + command);
			}
		send.flush();
		so.close();
		} catch (IOException e) {
			System.err.println("Socket-Error!");
			return;
		}

		System.out.println(so.getInetAddress().getHostAddress() + ": Close.");
	}

	/**
	 * @param command http-command
	 * @return list of randomly generated names
	 * Parses command given to a thread
	 */
	private String[] ParseCommand(String command) {
		String pattern, gender;
		int count, type;
		String[] mynames = { I18N.geti18nString(Messages.getString("NamegenServerThread.GenericError")) }; //$NON-NLS-1$
		
		StringReader r = new StringReader(command);
		StreamTokenizer st = new StreamTokenizer(r);
		try {
			//First arguement has to be GET
			type = st.nextToken();
			if (type != StreamTokenizer.TT_WORD) throw new IOException(I18N.geti18nString(Messages.getString("NamegenServerThread.Arg1"))); //$NON-NLS-1$
			if (!st.sval.equals("GET")) throw new IOException(I18N.geti18nString(Messages.getString("NamegenServerThread.Arg1"))); //$NON-NLS-2$
			
			//now pattern
			st.nextToken();
			if (st.sval == null || st.sval.equals("")) throw new IOException(I18N.geti18nString(Messages.getString("NamegenServerThread.Arg2"))); //$NON-NLS-2$
			pattern = st.sval;
			
			//now gender
			st.nextToken();
			if (st.sval == null || st.sval.equals("")) throw new IOException(I18N.geti18nString(Messages.getString("NamegenServerThread.Arg3"))); //$NON-NLS-2$
			gender = st.sval;
			
			//at last the number of arguements
			type = st.nextToken();
			if (type != StreamTokenizer.TT_NUMBER) throw new IOException(I18N.geti18nString(Messages.getString("NamegenServerThread.Arg4"))); //$NON-NLS-1$
			count = (int) st.nval;
		} catch (IOException e) {
			System.err.println(I18N.geti18nString(Messages.getString("NamegenServerThread.CommandError"))); //$NON-NLS-1$
			mynames[0] = I18N.geti18nString(Messages.getString("NamegenServerThread.CommandErrorStart")) + e.getMessage() + I18N.geti18nString(Messages.getString("NamegenServerThread.CommandErrorEnd")); //$NON-NLS-1$ //$NON-NLS-2$
			return mynames;
		}
		
		//ok, everything fine - now get names
		try {
			//synchronize name generation
			synchronized (ng) {
				mynames = ng.getRandomName(pattern, gender, count);
			}
		} catch(Exception e) {
			e.printStackTrace();
			mynames[0] = I18N.geti18nString(Messages.getString("NamegenServerThread.GeneratorError")); //$NON-NLS-1$
			return mynames;
		}
		return mynames;
	}
}
