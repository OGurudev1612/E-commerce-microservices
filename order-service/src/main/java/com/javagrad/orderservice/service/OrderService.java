package com.javagrad.orderservice.service;

import com.javagrad.orderservice.dto.InventoryResponse;
import com.javagrad.orderservice.dto.OrderLineItemsDto;
import com.javagrad.orderservice.dto.OrderRequest;
import com.javagrad.orderservice.event.OrderPlacedEvent;
import com.javagrad.orderservice.model.Order;
import com.javagrad.orderservice.model.OrderLineItems;
import com.javagrad.orderservice.repository.OrderRepository;
import io.micrometer.tracing.Span;
import io.micrometer.tracing.Tracer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
@Transactional
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final WebClient.Builder webClientBuilder;
    private final Tracer tracer;
    private final KafkaTemplate<String, OrderPlacedEvent> kafkaTemplate;

    public String placeOrder(OrderRequest orderRequest) {
        Order order = new Order();
        order.setOrderNumber(UUID.randomUUID().toString());

        List<OrderLineItems> orderLineItems = orderRequest.getOrderLineItemsDtoList()
                .stream()
                .map(this::mapToOrderLineItems)
                .toList();
        order.setOrderLineItemsList(orderLineItems);

        List<String> skuCodes =  orderLineItems.stream()
                .map(OrderLineItems::getSkuCode)
                .toList();

        // Call Inventory service and place order only if all the products are in stock
        Span inventoryServiceLookup = this.tracer.nextSpan().name("InventoryServiceLookup");
        try (Tracer.SpanInScope isl = tracer.withSpan(inventoryServiceLookup.start())) {
            InventoryResponse[] inventoryResponses = webClientBuilder.build()
                    .get()
                    .uri("http://inventory-service/api/inventory",
                            uriBuilder -> uriBuilder.queryParam("skuCode", skuCodes).build())
                    .retrieve()
                    .bodyToMono(InventoryResponse[].class)
                    .block();

            boolean allProductsInStock = false;
            if (inventoryResponses != null && inventoryResponses.length != 0) {
                allProductsInStock = Arrays.stream(inventoryResponses)
                        .allMatch(InventoryResponse::isInStock);
            }

            if (allProductsInStock) {
                orderRepository.save(order);
                kafkaTemplate.send("notificationTopic", new OrderPlacedEvent(order.getOrderNumber()));
                return "Order Placed Successfully";
            } else {
                log.error("Product is not in stock, please try again later");
                throw new IllegalArgumentException("Product is not in stock, please try again later");
            }
        }
        finally {
            inventoryServiceLookup.end();
        }
    }

    private OrderLineItems mapToOrderLineItems(OrderLineItemsDto orderLineItemsDto) {
        OrderLineItems orderLineItems = new OrderLineItems();
        orderLineItems.setSkuCode(orderLineItemsDto.getSkuCode());
        orderLineItems.setPrice(orderLineItemsDto.getPrice());
        orderLineItems.setQuantity(orderLineItemsDto.getQuantity());
        return orderLineItems;
    }

}
