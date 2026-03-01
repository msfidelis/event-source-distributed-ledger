package utils

import (
	"testing"
)

func TestRoundMoney(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected float64
	}{
		{"Já arredondado", 100.00, 100.00},
		{"Uma casa decimal", 100.5, 100.50},
		{"Duas casas decimais", 100.55, 100.55},
		{"Três casas decimais - arredonda para baixo", 100.554, 100.55},
		{"Três casas decimais - arredonda para cima", 100.555, 100.56},
		{"Muitas casas decimais", 56274.66158617926, 56274.66},
		{"Negativo", -50.555, -50.56},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RoundMoney(tt.input)
			if result != tt.expected {
				t.Errorf("RoundMoney(%f) = %f; esperado %f", tt.input, result, tt.expected)
			}
		})
	}
}

func TestRoundMoneyUp(t *testing.T) {
	tests := []struct {
		name     string
		input    float64
		expected float64
	}{
		{"Já arredondado", 100.00, 100.00},
		{"Uma casa decimal", 100.5, 100.50},
		{"Duas casas decimais", 100.55, 100.55},
		{"Três casas decimais - sempre para cima", 100.551, 100.56},
		{"Três casas decimais - sempre para cima 2", 100.554, 100.56},
		{"Muitas casas decimais - exemplo real", 56274.66158617926, 56274.67},
		{"Valor pequeno", 0.001, 0.01},
		{"Valor muito pequeno", 0.0001, 0.01},
		{"Zero", 0.0, 0.0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := RoundMoneyUp(tt.input)
			if result != tt.expected {
				t.Errorf("RoundMoneyUp(%f) = %f; esperado %f", tt.input, result, tt.expected)
			}
		})
	}
}

func TestRoundMoneyUpWithRealScenarios(t *testing.T) {
	// Simula operações bancárias reais
	tests := []struct {
		name            string
		saldoInicial    float64
		operacao        float64
		tipo            string
		saldoEsperado   float64
	}{
		{
			name:          "Depósito com valor quebrado",
			saldoInicial:  1000.00,
			operacao:      150.336,
			tipo:          "credito",
			saldoEsperado: 1150.34, // 1000.00 + 150.34 = 1150.34
		},
		{
			name:          "Saque com valor quebrado",
			saldoInicial:  1000.00,
			operacao:      25.447,
			tipo:          "debito",
			saldoEsperado: 974.55, // 1000.00 - 25.45 = 974.55 (arredondado para cima)
		},
		{
			name:          "Múltiplas operações",
			saldoInicial:  5000.123,
			operacao:      1274.54058617926,
			tipo:          "credito",
			saldoEsperado: 6274.68, // 5000.13 + 1274.55 = 6274.68 (arredondado para cima)
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			saldo := RoundMoneyUp(tt.saldoInicial)
			valor := RoundMoneyUp(tt.operacao)
			
			var novoSaldo float64
			if tt.tipo == "credito" {
				novoSaldo = RoundMoneyUp(saldo + valor)
			} else {
				novoSaldo = RoundMoneyUp(saldo - valor)
			}

			if novoSaldo != tt.saldoEsperado {
				t.Errorf("Saldo final = %.2f; esperado %.2f (saldo=%f, valor=%f)", novoSaldo, tt.saldoEsperado, saldo, valor)
			}
		})
	}
}
