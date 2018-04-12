package configuration;

import com.amazonaws.ClientConfiguration;

public class ConfigurationUtils {

	private static final String APPLICATION_NAME = "kinesis-barebones";
	private static final String VERSION = "1.0.0";

	public static ClientConfiguration getClientConfigWithUserAgent() {
		final ClientConfiguration config = new ClientConfiguration();
		final StringBuilder userAgent = new StringBuilder(ClientConfiguration.DEFAULT_USER_AGENT);
		userAgent.append(" ");
		userAgent.append(APPLICATION_NAME);
		userAgent.append("/");
		userAgent.append(VERSION);
		config.setUserAgentPrefix(userAgent.toString());
		config.setUserAgentSuffix(null);
		return config;
	}

}
