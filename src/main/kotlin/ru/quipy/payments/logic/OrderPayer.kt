package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import ru.quipy.common.utils.TooManyRequestsException
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*

@Service
class OrderPayer {
    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }
    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        //TODO:
        // подумать про поведение в случае ошибок между компонентами данного метода
        paymentService.checkDeadline(deadline)

//        try {
            val createdEvent = paymentESService.create {
                it.create(
                    paymentId,
                    orderId,
                    amount
                )
            }
            logger.trace("Payment {} for order {} created.", createdEvent.paymentId, orderId)
//
//        } catch (e: Exception) {
//            logger.error("MAKE SENSE")
//            paymentService.foo(deadline)
//        }

        paymentService.submitPaymentRequest(
            paymentId,
            amount,
            createdAt,
            deadline
        )
        return createdAt
    }
}