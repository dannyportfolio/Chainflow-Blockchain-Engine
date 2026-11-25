// SPDX-License-Identifier: MIT
pragma solidity ^0.8.24;

contract TestCounter {
    uint256 public count;

    event Incremented(address indexed caller, uint256 newCount);

    function increment() external {
        count += 1;
        emit Incremented(msg.sender, count);
    }

    function alwaysRevert() external pure {
        revert("intentional revert");
    }
}
