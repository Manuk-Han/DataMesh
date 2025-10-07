package module.registry.schemaregistry.controller;

import lombok.RequiredArgsConstructor;
import module.contract.catalog.dto.SchemaDto;
import module.registry.schemaregistry.entity.SchemaEntity;
import module.registry.schemaregistry.mapper.SchemaMapper;
import module.registry.schemaregistry.repository.SchemaRepository;
import org.springframework.http.ResponseEntity;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/catalog/schema")
@RequiredArgsConstructor
public class SchemaController {
    private final SchemaRepository schemaRepository;
    private final SchemaMapper schemaMapper;

    @GetMapping("/{name}")
    public ResponseEntity<?> get(@PathVariable String name){
        return schemaRepository.findTopByNameOrderByVersionDesc(name)
                .map(schemaMapper::toDto)
                .<ResponseEntity<?>>map(ResponseEntity::ok)
                .orElseGet(() -> ResponseEntity.notFound().build());
    }

    @PostMapping
    @Transactional
    public ResponseEntity<?> upsert(@Validated @RequestBody SchemaDto req) {
        int nextVersion = schemaRepository.findTopByNameOrderByVersionDesc(req.getName())
                .map(SchemaEntity::getVersion).orElse(0) + 1;

        SchemaEntity saved = schemaRepository.save(schemaMapper.toNewEntityForUpsert(req, nextVersion));
        return ResponseEntity.ok(schemaMapper.toDto(saved));
    }
}
