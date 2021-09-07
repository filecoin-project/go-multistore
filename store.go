package multistore

import (
	"github.com/ipfs/go-blockservice"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-graphsync/storeutil"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	offline "github.com/ipfs/go-ipfs-exchange-offline"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/ipfs/go-merkledag"
	ipldprime "github.com/ipld/go-ipld-prime"
)

// Store is a single store instance returned by the MultiStore.
// it gives public access to the blockstore, dag service,
// and an ipld-prime linksystem
type Store struct {
	ds datastore.Batching

	Bstore blockstore.Blockstore

	bsvc       blockservice.BlockService
	DAG        ipld.DAGService
	LinkSystem ipldprime.LinkSystem
}

func openStore(ds datastore.Batching) (*Store, error) {
	blocks := namespace.Wrap(ds, datastore.NewKey("blocks"))
	bs := blockstore.NewBlockstore(blocks)

	ibs := blockstore.NewIdStore(bs)

	bsvc := blockservice.New(ibs, offline.Exchange(ibs))
	dag := merkledag.NewDAGService(bsvc)

	lsys := storeutil.LinkSystemForBlockstore(ibs)

	return &Store{
		ds: ds,

		Bstore: ibs,

		bsvc:       bsvc,
		DAG:        dag,
		LinkSystem: lsys,
	}, nil
}

// Close closes down the blockservice used by the DAG Service for this store
func (s *Store) Close() error {
	return s.bsvc.Close()
}
