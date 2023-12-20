package io.smartdatalake.util.crypt

import org.apache.hadoop.hive.ql.exec.UDF

class EncDecECB extends EncryptDecryptSupport {
  def encrypt(message: String, key: String, algorithm: String): String = {
    val keyBytes: Array[Byte] = key.getBytes
    val crypt: EncryptDecrypt = algorithm match {
      case "GCM" => new EncryptDecryptGCM(keyBytes)
      case "ECB" => new EncryptDecryptECB(keyBytes)
      case classname if classname.contains(".") => loadEncryptDecryptClass(classname, keyBytes)
      case _ => throw new UnsupportedOperationException(s"unsupported en/decryption algorithm ${algorithm}")
    }
    crypt.encrypt(message)
  }

  def decrypt(message: String, key: String, algorithm: String): String = {
    val keyBytes: Array[Byte] = key.getBytes
    val crypt: EncryptDecrypt = algorithm match {
      case "GCM" => new EncryptDecryptGCM(keyBytes)
      case "ECB" => new EncryptDecryptECB(keyBytes)
      case classname if classname.contains(".") => loadEncryptDecryptClass(classname, keyBytes)
      case _ => throw new UnsupportedOperationException(s"unsupported en/decryption algorithm ${algorithm}")
    }
    crypt.decrypt(message)
  }
}