package net.broscorp.web3.dto.request;

import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.List;

/**
 * A request specifically for the "logs" dataset.
 * Contains additional, optional filtering parameters for logs:
 * <ul>
 *     <li>
 *         contractAddresses
 *     </li>
 *     <li>
 *          topics
 *     </li>
 * </ul>
 */
@Data
@EqualsAndHashCode(callSuper = true)
public final class LogsRequest extends ClientRequest {
    private List<String> contractAddresses;
    private List<String> topics;
}
