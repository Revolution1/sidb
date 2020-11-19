package sidb

func Set(b, flag uint8) uint8    { return b | flag }
func Clear(b, flag uint8) uint8  { return b &^ flag }
func Toggle(b, flag uint8) uint8 { return b ^ flag }
func Has(b, flag uint8) bool     { return b&flag != 0 }
