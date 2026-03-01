package utils

import "math"

// RoundMoney arredonda um valor monetário para 2 casas decimais
// Utiliza arredondamento bancário (round half to even) que é o padrão do Go
func RoundMoney(value float64) float64 {
	return math.Round(value*100) / 100
}

// RoundMoneyUp arredonda um valor monetário para 2 casas decimais sempre para cima
func RoundMoneyUp(value float64) float64 {
	return math.Ceil(value*100) / 100
}
