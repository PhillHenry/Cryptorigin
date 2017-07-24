package uk.co.odinconsultants.bitcoin.parsing

import uk.co.odinconsultants.bitcoin.parsing.Indexer._

trait MetaStore extends ((BackReference, PubKey) => Unit)