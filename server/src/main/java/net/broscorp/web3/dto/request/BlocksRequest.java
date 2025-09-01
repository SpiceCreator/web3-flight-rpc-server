package net.broscorp.web3.dto.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

/**
 * A request specifically for the "blocks" dataset.
 * It has no additional fields beyond the base request.
 */
@Data
@EqualsAndHashCode(callSuper = true)
public final class BlocksRequest extends ClientRequest {
}
