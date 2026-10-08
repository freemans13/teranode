package netsync

import (
	"encoding/json"
	"errors"
	"io/fs"
	"os"
)

// peerRatesFile keeps each peer address's last measured download rate across a restart, beside
// the address manager's peers.json. The deadline rule then starts known peers at their speed
// instead of handing near blocks to a peer that turns out to be slow.
const peerRatesFile = "legacy-peer-rates.json"

// loadPeerRates reads the rates file. No file is an empty memory, not an error.
func loadPeerRates(path string) (map[string]float64, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return map[string]float64{}, nil
	}

	if err != nil {
		return nil, err
	}

	rates := map[string]float64{}
	if err := json.Unmarshal(b, &rates); err != nil {
		return nil, err
	}

	return rates, nil
}

// savePeerRates writes through a temporary file and a rename, so a crash leaves the previous file.
func savePeerRates(path string, rates map[string]float64) error {
	b, err := json.Marshal(rates)
	if err != nil {
		return err
	}

	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}

	return os.Rename(tmp, path)
}

// peerRatesSaveEvery is how many race ticks (raceCheckInterval, 5 s) go between two writes of the
// rates file: 10 minutes.
const peerRatesSaveEvery = 120

// savePeerRates writes each peer address's rate to the rates file, when the node keeps one.
func (sm *SyncManager) savePeerRates() {
	if sm.peerRatesPath == "" || sm.streams == nil {
		return
	}

	if err := savePeerRates(sm.peerRatesPath, sm.streams.rememberedRates()); err != nil {
		sm.logger.Warnf("[legacy] could not write %s: %v", sm.peerRatesPath, err)
	}
}
