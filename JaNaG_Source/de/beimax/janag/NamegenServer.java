/**
 * $Id: NamegenServer.java 5 2008-12-23 15:34:48Z ronix $
 * File: NamegenServer.java
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

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * @author mkalus
 * Starter Class to start a Namegenerator server.
 */
public class NamegenServer {

	/**
	 * @param args expects up to one arguement (socket number)
	 */
	public static void main(String[] args) {
		int port = 12022;
		
		//Ask command line
		if (args.length == 1) {//Port als Argument
			port = Integer.parseInt(args[0]);
		} else if (args.length > 1) {
			System.err.println("Too many arguements - only considering the first"); //$NON-NLS-1$
		}

		try {
			ServerSocket seso = new ServerSocket(port);
			System.out.println("Starting server on port " + port + "..."); //$NON-NLS-1$
			System.out.println("Expected request form: GET \"PATTERN\" \"GENDER\" COUNT"); //$NON-NLS-1$
			
			while(true) {
				Socket so = seso.accept();
				Thread worker = new NamegenServerThread(so);
				worker.start();
			}
		} catch (IOException e) {
			System.err.println("Server IO Error"); //$NON-NLS-1$
			System.exit(1);
		}
		System.out.println("Stopping Server..."); //$NON-NLS-1$
	}

}
