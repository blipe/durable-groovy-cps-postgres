def payment = step('charge-card', [
    orderId: input('orderId'),
    amount: input('amount')
], io.github.durablecps.api.StepOptions.retry(3, java.time.Duration.ofSeconds(2)))

def approval = awaitSignal('approve-order')
if (approval != 'approved') {
    return [status: 'rejected', payment: payment]
}

sleepMillis(250)
def shipment = step('create-shipment', [orderId: input('orderId')])
return [status: 'shipped', payment: payment, shipment: shipment]
