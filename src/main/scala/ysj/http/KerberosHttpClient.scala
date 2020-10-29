package ysj.http

import java.io.IOException
import java.security.{Principal, PrivilegedAction}
import java.util

import javax.security.auth.Subject
import javax.security.auth.kerberos.KerberosPrincipal
import javax.security.auth.login.{AppConfigurationEntry, Configuration, LoginContext}
import org.apache.commons.io.IOUtils
import org.apache.http.HttpResponse
import org.apache.http.auth.{AuthSchemeProvider, AuthScope, Credentials}
import org.apache.http.client.HttpClient
import org.apache.http.client.config.AuthSchemes
import org.apache.http.client.methods.{HttpGet, HttpRequestBase}
import org.apache.http.config.{Lookup, RegistryBuilder}
import org.apache.http.impl.auth.SPNegoSchemeFactory
import org.apache.http.impl.client.{BasicCredentialsProvider, HttpClientBuilder}

object KerberosHttpClient {

  def main(args: Array[String]): Unit = {
    val response = request(new HttpGet("http://test.com/service/employees"),
                           "HTTP/test@test.com",
                           "HTTP/test@test.com",
                           "file://C://Development//test.keytab")

    val inputStream = response.getEntity.getContent
    println("Status code " + response.getStatusLine.getStatusCode)
    println(util.Arrays.deepToString(response.getAllHeaders.toArray))
    println(new String(IOUtils.toByteArray(inputStream), "UTF-8"))
  }

  def request(httpRequest: HttpRequestBase, userId: String, principal: String, keyTabLocation: String): HttpResponse = {
    val config: Configuration = new Configuration() {
      override def getAppConfigurationEntry(name: String): Array[AppConfigurationEntry] =
        Array(
          new AppConfigurationEntry(
            "com.sun.security.auth.module.Krb5LoginModule",
            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
            new java.util.HashMap[String, Object] {
              put("useTicketCache", "false")
              put("useKeyTab", "true")
              put("keyTab", keyTabLocation)
              put("refreshKrb5Config", "true")
              put("principal", principal)
              put("storeKey", "true")
              put("doNotPrompt", "true")
              put("isInitiator", "true")
              put("debug", "true")
            }
          )
        )
    }

    val princ: util.Set[Principal] = new util.HashSet[Principal](1)
    princ.add(new KerberosPrincipal(userId))
    val sub = new Subject(false, princ, new util.HashSet[AnyRef], new util.HashSet[AnyRef])
    try {
      val lc = new LoginContext("", sub, null, config)
      lc.login()
      val serviceSubject = lc.getSubject
      val httpResponse = Subject.doAs(
        serviceSubject,
        new PrivilegedAction[HttpResponse]() {
          override def run(): HttpResponse =
            try {
              val spnegoHttpClient = buildSpengoHttpClient
              spnegoHttpClient.execute(httpRequest)
            } catch {
              case ioe: IOException =>
                ioe.printStackTrace()
                null
            }
        }
      )
      return httpResponse
    } catch {
      case le: Exception => le.printStackTrace()
    }
    null
  }

  private def buildSpengoHttpClient(): HttpClient = {
    val builder: HttpClientBuilder = HttpClientBuilder.create
    val authSchemeRegistry: Lookup[AuthSchemeProvider] =
      RegistryBuilder.create[AuthSchemeProvider].register(AuthSchemes.SPNEGO, new SPNegoSchemeFactory(true)).build
    builder.setDefaultAuthSchemeRegistry(authSchemeRegistry)
    val credentialsProvider: BasicCredentialsProvider = new BasicCredentialsProvider
    credentialsProvider.setCredentials(new AuthScope(null, -(1), null), new Credentials() {
      def getUserPrincipal: Principal = null
      def getPassword: String         = null
    })
    builder.setDefaultCredentialsProvider(credentialsProvider)
    builder.build
  }
}
