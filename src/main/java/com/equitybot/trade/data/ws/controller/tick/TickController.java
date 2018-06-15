package com.equitybot.trade.data.ws.controller.tick;

import java.util.ArrayList;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.equitybot.trade.data.db.mongodb.tick.domain.Tick;
import com.equitybot.trade.data.db.mongodb.tick.repository.TickRepository;
import com.mongodb.client.result.DeleteResult;

@RestController
@RequestMapping("/api")
public class TickController {

	@Autowired
	private TickRepository tickRepository;

	@PostMapping("/tick/v1.0")
	public void saveTickList(@RequestBody ArrayList<Tick> ticks) {
		tickRepository.saveAllTick(ticks);
	}
	@GetMapping("/tick/v1.0")
	public List<List<Tick>> getAllTicks(@RequestBody ArrayList<Long> instrumentTokens) {
		return tickRepository.findAllTicksByInstrumentToken(instrumentTokens);
		
	}
	@GetMapping("/tick/v1.0/{instrumentToken}")
	public List<Tick> getTicksByInstrumentToken(@PathVariable long instrumentToken) {
		return tickRepository.findByInstrumentToken(instrumentToken);
	}
	@GetMapping("/tick/v1.0/findById/{id}")
	public List<Tick> getTicksByInstrumentToken(@PathVariable String id) {
		return tickRepository.findById(id);
	}
	
	@DeleteMapping({ "/tick/v1.0" })
	public DeleteResult delete() {
		return tickRepository.deleteAllTicks();
	}
}
