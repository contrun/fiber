use crate::fiber::fee::calculate_tlc_forward_fee;

#[test]
fn test_zero_tlc_forward_fee() {
    let amount = 1000;
    let fee_proportational_millionths = 0;
    let fee = calculate_tlc_forward_fee(amount, fee_proportational_millionths);
    assert_eq!(fee, 0);
}
