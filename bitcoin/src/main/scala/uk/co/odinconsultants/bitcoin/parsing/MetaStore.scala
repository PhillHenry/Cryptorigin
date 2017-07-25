package uk.co.odinconsultants.bitcoin.parsing

import uk.co.odinconsultants.bitcoin.parsing.Indexer._
import uk.co.odinconsultants.bitcoin.parsing.MetaStore.Payload

trait MetaStore extends (Payload => Unit)

object MetaStore {
  type Payload = List[(BackReference, PubKey)]
}