/*
 * 
 */
package com.mfcgd.ranger.processor;

import java.io.*;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.util.List;
import java.util.regex.Pattern;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpResponseException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.mfcgd.ranger.dto.DataSource;
import com.mfcgd.ranger.dto.Policy;
import com.mfcgd.ranger.dto.User;
import com.mfcgd.ranger.dto.VXPolicies;

import org.apache.http.ssl.SSLContextBuilder;

import javax.net.ssl.SSLContext;

// TODO: Auto-generated Javadoc
/**
 * The Class Extractor.
 */
@SuppressWarnings("unused")
public class Extractor {

	/**
	 * The main method.
	 *
	 * @param args the arguments
	 */
	public static void main(String[] args) {
		InputStream input = null;
		try {

			Properties prop = new java.util.Properties();
			input = new FileInputStream(args[0]);
			if (input == null) {
				System.out.println("Sorry, unable to find config file " + args[0]);
				return;
			}

			prop.load(input);

			GetData(prop);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * Gets the data.
	 *
	 * @throws IOException Signals that an I/O exception has occurred.
	 */
	public static void GetData(Properties p) throws IOException {
		ResponseHandler<DataSource> data = new ResponseHandler<DataSource>() {
			@Override
			public DataSource handleResponse(final HttpResponse response) throws IOException {
				StatusLine statusLine = response.getStatusLine();
				HttpEntity entity = response.getEntity();
				if (statusLine.getStatusCode() >= 300) {
					throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
				}
				if (entity == null) {
					throw new ClientProtocolException("Response contains no content");
				}
				InputStream instream = entity.getContent();
				ObjectMapper mapper = new ObjectMapper();
				mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
				mapper.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
				mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
				try {
					DataSource dataSource = mapper.readValue(instream, DataSource.class);
					//System.out.println("response from url" + dataSource.toString());
					return dataSource;
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				} finally {
					instream.close();
				}
				return null;
			}
		};

		/*CloseableHttpClient httpclient = HttpClients.createDefault(); // .setSSLContext(sslContext);
		String credentials = p.getProperty("userName") + ":" + p.getProperty("userPassword");
		String authHdr = java.util.Base64.getEncoder().encodeToString(credentials.getBytes("utf-8"));
*/
	/*	HttpGet req = new HttpGet(p.getProperty("hivePolicyUrl"));
		req.addHeader("Authorization", "Basic " + authHdr);
		DataSource dataHive = httpclient.execute(req, data);

		req = new HttpGet(p.getProperty("hadoopPolicyUrl"));
		req.addHeader("Authorization", "Basic " + authHdr);
		DataSource dataHadoop = httpclient.execute(new HttpGet(p.getProperty("hadoopPolicyUrl")), data);
		System.out.println("Extraction success.");

		System.out.println("Hadoop" + dataHadoop.toString());
		System.out.println("datahive" + dataHive.toString());*/

		//getPolicyDescription(p, dataHive);
	//	getPolicyDescription(p, dataHadoop);
		System.out.println("DataOwner Extraction done.");

		Transformer tf = new Transformer();
		List<User> userList = tf.Transform(getPolicyDescription(p));
		userList.addAll(tf.Transform(getPolicyDescription(p)));
		System.out.println("Transformation success.");

		Loader tp = new Loader();
		String path = tp.OutputToCsv(userList, tf, p);

		System.out.println("Tool ran succesfully and has terminated.");
	}

	private static DataSource getPolicyDescription(Properties prop) throws java.io.UnsupportedEncodingException {

		String m_url = prop.getProperty("policyRestApi");
		String credentials = prop.getProperty("userName") + ":" + prop.getProperty("userPassword");
		String authHdr = java.util.Base64.getEncoder().encodeToString(credentials.getBytes("utf-8"));
		java.util.regex.Pattern p = java.util.regex.Pattern.compile(".*DataOwner=(\\w+)", Pattern.MULTILINE);

		ResponseHandler<List<Policy>> data = new ResponseHandler<List<Policy>>() {
			@Override
			public List<Policy> handleResponse(final HttpResponse response) throws IOException {
				StatusLine statusLine = response.getStatusLine();
				HttpEntity entity = response.getEntity();
				//System.out.println("Here is the policy response" + response.getEntity());
				if (statusLine.getStatusCode() >= 300) {
					// System.out.println
					throw new HttpResponseException(statusLine.getStatusCode(), statusLine.getReasonPhrase());
				}
				if (entity == null) {
					System.out.println("entity is null" + entity);
					throw new ClientProtocolException("Response contains no content");
				}
				ObjectMapper mapper = new ObjectMapper();
				mapper.enable(DeserializationFeature.ACCEPT_SINGLE_VALUE_AS_ARRAY);
				mapper.enable(DeserializationFeature.ACCEPT_EMPTY_ARRAY_AS_NULL_OBJECT);
				mapper.enable(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT);
				InputStream instream = entity.getContent();
				try {
					List<Policy> policy = mapper.readValue(instream, new TypeReference<List<Policy>>(){});
					//Policy policy = mapper.readValue(instream, Policy.class);
					return policy;
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} finally {
					instream.close();
				}
				return null;
			}
		};

		CloseableHttpClient httpclient = HttpClients.custom()
				.setDefaultRequestConfig(
						RequestConfig.custom().setConnectionRequestTimeout(10000).setConnectTimeout(5000)
								.setExpectContinueEnabled(false).setSocketTimeout(5000).setCookieSpec("easy").build())
				.setMaxConnPerRoute(20).setMaxConnTotal(100).build();
		DataSource policies = new DataSource();

		try {
			String url = m_url;
			HttpGet req = new HttpGet(url);
			req.addHeader("Authorization", "Basic " + authHdr);
			List<Policy> polices = httpclient.execute(req, data);
			/*java.util.regex.Matcher m = p.matcher(polices.);
			if (m.find())
				pl.setDescription(m.group(1));
			else
				pl.setDescription("N/A");*/
			policies.setPolicies(polices);
		} catch (java.io.IOException e) {
			System.out.println(e.getMessage());
		}
		return policies;
	}

	private static String ConverToString(final InputStream is) {
		final BufferedReader br = new BufferedReader(new InputStreamReader(is));
		final StringBuffer buffer = new StringBuffer();
		try {
			for (String line; (line = br.readLine()) != null;) {
				buffer.append(line);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return buffer.toString();
	}
}
