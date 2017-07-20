package uk.co.odinconsultants.bitcoin.core

trait Logging {

  def info(msg: String): Unit = output(msg) // TODO - some proper logging

  def error(msg: String): Unit = output(msg) // TODO - some proper logging

  def debug(msg: String): Unit = output(msg) // TODO proper debugging

  def output(msg: String): Unit = println(Thread.currentThread().getName + ": " + msg)


}
