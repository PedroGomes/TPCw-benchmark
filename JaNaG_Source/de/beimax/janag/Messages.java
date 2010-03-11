package JaNaG_Source.de.beimax.janag;

import java.util.MissingResourceException;
import java.util.ResourceBundle;

/**
 * @author mkalus
 * I18N-Message Class
 */
public class Messages {
	private static final String BUNDLE_NAME = "de.beimax.janag.messages"; //$NON-NLS-1$

	private static final ResourceBundle RESOURCE_BUNDLE = ResourceBundle
			.getBundle(BUNDLE_NAME);

	private Messages() {
	}

	public static String getString(String key) {
		try {
			return RESOURCE_BUNDLE.getString(key);
		} catch (MissingResourceException e) {
			return '!' + key + '!';
		}
	}
}
