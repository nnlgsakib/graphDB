import { MerkleTree } from 'merkletreejs';
import keccak256 from 'keccak256';

/**
 * Build a Merkle tree from chunk-hashes (prefixed 'nlg').
 */
export function buildMerkleTree(hashes: string[]) {
  const leaves = hashes.map((h) => Buffer.from(h.replace(/^nlg/, ''), 'hex'));
  const tree = new MerkleTree(leaves, keccak256, { sortPairs: true });
  const root = tree.getRoot().toString('hex');
  return { tree, root };
}

/**
 * Generate a partial proof for a single chunk-hash.
 */
export function generateMerkleProof(tree: MerkleTree, chunkHash: string) {
  const leaf = Buffer.from(chunkHash.replace(/^nlg/, ''), 'hex');
  return tree.getProof(leaf);
}
