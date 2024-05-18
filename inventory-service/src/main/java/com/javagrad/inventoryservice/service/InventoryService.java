package com.javagrad.inventoryservice.service;

import com.javagrad.inventoryservice.dto.InventoryResponse;
import com.javagrad.inventoryservice.repository.InventoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class InventoryService {

    private final InventoryRepository inventoryRepository;

    @Transactional(readOnly = true)
    public List<InventoryResponse> isInStock(List<String> skuCode) {
        return inventoryRepository.findBySkuCodeIn(skuCode).stream()
                .map(Inventory -> InventoryResponse.builder()
                        .skuCode(Inventory.getSkuCode())
                        .isInStock(Inventory.getQuantity() > 0)
                        .build()
                )
                .toList();
    }

}
