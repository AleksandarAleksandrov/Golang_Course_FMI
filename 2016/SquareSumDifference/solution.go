package main

/*
// this variant although much prettier can overflow quite fast
func SquareSumDifference(n uint64) uint64 {
	sumOfSquares := n * (n + 1) * (2*n + 1) / 6
	squareOfSum := n * n * (n + 1) * (n + 1) / 4
	return squareOfSum - sumOfSquares
}*/

func SquareSumDifference(n uint64) uint64 {
	var sumOfSquares uint64
	var squareOfSum uint64
	var count uint64

	for ; count < n+1; count++ {
		sumOfSquares += count * count
		squareOfSum += count
	}

	return squareOfSum*squareOfSum - sumOfSquares
}
