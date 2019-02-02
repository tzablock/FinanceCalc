import scala.annotation.tailrec

@tailrec
val calculateCompoundIncome = (initAmt: Double, period: Int, interestRate: Double) => {
  if (period == 0){
    initAmt + interestRate*initAmt
  } else {
    calculateCompoundIncome(initAmt + interestRate*initAmt,period-1,interestRate)
  }
}

calculateCompoundIncome(100,3,0.5)