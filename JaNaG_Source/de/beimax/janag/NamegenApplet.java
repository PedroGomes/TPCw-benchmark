/**
 * $Id: NamegenApplet.java 5 2008-12-23 15:34:48Z ronix $
 * File: NamegenApplet.java
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

import java.applet.Applet;
import java.awt.Button;
import java.awt.Choice;
import java.awt.TextArea;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;

/**
 * @author mkalus
 * GUI Application as applet
 */
public class NamegenApplet extends Applet implements ActionListener,
		ItemListener {
	private static final long serialVersionUID = -2251357454343303256L;

	private Namegenerator ng;

	private Button butGenerate;
	private Choice chPattern, chGender, chCount;
	private TextArea txtOutput;

	private String strPattern;

	/* (non-Javadoc)
	 * @see java.applet.Applet#init()
	 */
	public void init() {
		setLayout(null);
		String mylang = getParameter("language");
		if (mylang != null)
			ng = new Namegenerator("languages.txt", "semantics.txt", mylang); //$NON-NLS-1$ //$NON-NLS-2$
		else
			ng = new Namegenerator("languages.txt", "semantics.txt"); //$NON-NLS-1$ //$NON-NLS-2$
		this.resize(210,355);
		//setBackground(Color.yellow);
		//setForeground(Color.orange);

		chPattern = new Choice();
		String[] list = ng.getPatterns();

		for (int i = 0; i < list.length; i++)
			chPattern.addItem(list[i]);

		chPattern.addItemListener(this);
		chPattern.setBounds(5,5,200,20);
		add(chPattern);
		strPattern = list[0];

		chGender = new Choice();
		list = ng.getGenders(strPattern);

		for (int i = 0; i < list.length; i++)
			chGender.addItem(list[i]);

		chGender.addItemListener(this);
		chGender.setBounds(5,35,200,20);
		add(chGender);

		chCount = new Choice();
		String[] adder = {"1", "2", "3", "4", "5", "10", "20", "30", //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$ //$NON-NLS-4$ //$NON-NLS-5$ //$NON-NLS-6$ //$NON-NLS-7$ //$NON-NLS-8$
			"40", "50", "100"}; //$NON-NLS-1$ //$NON-NLS-2$ //$NON-NLS-3$

		for (int i = 0; i < adder.length; i++)
			chCount.addItem(adder[i]);
		chCount.setBounds(5,65,50,20);
		add(chCount);

		butGenerate = new Button(I18N.geti18nString(Messages.getString("Namegen0"))); //$NON-NLS-1$
		butGenerate.addActionListener(this);
		butGenerate.setBounds(80,65,100,20);
		add(butGenerate);

		txtOutput = new TextArea(10, 40);
		txtOutput.setBounds(5,100,200,250);
		add(txtOutput);


	}

	/* (non-Javadoc)
	 * @see java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
	 */
	public void actionPerformed (ActionEvent ae) {
		if (ae.getSource() == butGenerate) {
			int zahl = Integer.parseInt(chCount.getSelectedItem());
			String[] mynames = ng.getRandomName(chPattern.getSelectedItem(),
				chGender.getSelectedItem(), zahl);

			for (int i = 0; i < zahl; i++)
				txtOutput.append(mynames[i] + "\r\n");
		}
	}

	/* (non-Javadoc)
	 * @see java.awt.event.ItemListener#itemStateChanged(java.awt.event.ItemEvent)
	 */
	public void itemStateChanged(ItemEvent ie) {
		if (ie.getSource() == chPattern) { //change pattern
			chGender.removeAll();

			if (strPattern == chPattern.getSelectedItem()) return;
			strPattern = chPattern.getSelectedItem();
			String[] list = ng.getGenders(strPattern);

			for (int i = 0; i < list.length; i++)
				chGender.addItem(list[i]);
		}
	}
}
