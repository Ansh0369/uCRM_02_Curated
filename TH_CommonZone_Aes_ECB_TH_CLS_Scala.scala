// Databricks notebook source
import org.apache.spark.sql.functions.udf
import java.util.Date
import java.text.SimpleDateFormat
import org.apache.commons.lang3.time.DateFormatUtils
import java.util.Calendar
import java.security.SecureRandom
import javax.crypto.Cipher
import javax.crypto.KeyGenerator
import javax.crypto.SecretKey
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.hadoop.hive.ql.exec.UDF
import sun.misc.BASE64Decoder
import sun.misc.BASE64Encoder
import sun.misc.{BASE64Decoder, BASE64Encoder}
import javax.crypto.spec.SecretKeySpec
import java.security.Security
import java.nio.charset.StandardCharsets
import java.security.{MessageDigest, NoSuchAlgorithmException}
import java.util
import java.util.{Arrays, Base64}
import javax.crypto.{Cipher, SecretKey}
import javax.crypto.spec.SecretKeySpec
import org.bouncycastle.jce.provider.BouncyCastleProvider

// COMMAND ----------

//20230319 use object to create a new single instance

class SecretEncryptUtils(private val key:String, private val secretKey: SecretKeySpec) extends Serializable {

  def EncryptEncode(strToEncrypt: String): String = {
    if (strToEncrypt == null){
            return strToEncrypt

    }
//     if(strToEncrypt.trim.length == 0){
//       return strToEncrypt.trim
//     }

      val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
      cipher.init(Cipher.ENCRYPT_MODE, secretKey)
      return Base64.getEncoder.encodeToString(cipher.doFinal(strToEncrypt.getBytes(StandardCharsets.UTF_8)))

  }

  def DecryptDecode(strToEncrypt: String): String = {

    if (strToEncrypt == null){
      return strToEncrypt
    }
//     if(strToEncrypt.trim.length == 0){
//       return strToEncrypt.trim
//     }
    
      
      val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
      cipher.init(Cipher.DECRYPT_MODE, secretKey)
      val byteContent: Array[Byte] = new BASE64Decoder().decodeBuffer(strToEncrypt)

      val byteDecode:Array[Byte] = cipher.doFinal(byteContent)
      return new String(byteDecode, java.nio.charset.StandardCharsets.UTF_8)

  }

}


object SecretEncryptUtils {
  
  Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider())
  
  java.security.Security.setProperty("crypto.policy", "unlimited")

  def apply():SecretEncryptUtils = { 
    val key = dbutils.secrets.get(scope = "kv_gdp_eas_fwdapp_01_scopename", key = "kafka-th-PII-RTIngestion--Encryption-key")
    val secretKey: SecretKeySpec =  new SecretKeySpec(key.getBytes(), "AES")
    new SecretEncryptUtils(key, secretKey)
  }

}

val secretEncryptUtils = SecretEncryptUtils()

spark.udf.register("TH_encrypt", secretEncryptUtils.EncryptEncode(_: String))
spark.udf.register("TH_decrypt", secretEncryptUtils.DecryptDecode(_: String))


// COMMAND ----------

