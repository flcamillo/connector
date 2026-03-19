package internal

import "fmt"

// Estrutura do evento de agendamento disparado via AWS EventBridge
type SchedulerEvent struct {
	Command string `json:"codigoComando"`
	Mft     string `json:"codigoMft"`
}

// Valida os campos do registro.
func (p *SchedulerEvent) Validate() error {
	if p.Command == "" {
		return fmt.Errorf("invalid command")
	}
	if p.Mft == "" {
		return fmt.Errorf("invalid mft")
	}
	return nil
}
