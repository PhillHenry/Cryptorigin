package uk.co.odinconsultants.bitcoin.parsing

import uk.co.odinconsultants.bitcoin.parsing.Indexer._
import uk.co.odinconsultants.bitcoin.parsing.MetaStore.Batch

trait MetaStore extends (Batch => Unit)

object MetaStore {
  type Payload  = (BackReference, PubKey)
  type Batch    = List[Payload]
}