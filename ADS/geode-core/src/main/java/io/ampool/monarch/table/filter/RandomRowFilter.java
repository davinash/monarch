package io.ampool.monarch.table.filter;

import java.util.Random;

import io.ampool.classification.InterfaceAudience;
import io.ampool.classification.InterfaceStability;
import io.ampool.monarch.table.Row;

/**
 * A filter that includes row based on a chance.
 *
 * This filter is useful to get random rows from the table, useful in random sampling use cases.
 * <P>
 * </P>
 * Obtain random set of table rows using a filter :
 * <P>
 * </P>
 * MScan scan = new MScan();<br>
 * MFilter filter = new RandomRowFilter(0.3);<br>
 * scan.setFilter(filter);<br>
 * MResultScanner scanner = table.getScanner(scan);
 * <P>
 * </P>
 */

@InterfaceAudience.Public
@InterfaceStability.Stable
public class RandomRowFilter extends FilterBase {
  protected static final Random random = new Random();
  private static final long serialVersionUID = -3442902393232301623L;

  protected float chance;
  protected boolean filterRow;

  /**
   * Create a new filter with a specified chance for a row to be included.
   *
   * @param chance a float value between 0 and 1
   */
  public RandomRowFilter(float chance) {
    this.chance = chance;
  }

  /**
   * @return The chance that a row gets included.
   */
  public float getChance() {
    return chance;
  }

  /**
   * Set the chance that a row is included.
   *
   * @param chance
   */
  public void setChance(float chance) {
    this.chance = chance;
  }

  @Override
  public boolean filterAllRemaining() {
    return false;
  }

  @Override
  public boolean filterRow() {
    return filterRow;
  }

  @Override
  public boolean hasFilterRow() {
    return true;
  }

  @Override
  public boolean hasFilterCell() {
    return false;
  }

  /**
   * We use a {@link Random} and return true is chance is less than the next random value returned,
   * false otherwise. If chance is < 0 always filter (return no rows); if chance is > 1 always
   * include (return all rows).
   *
   * @param result the row to evaluate
   * @return true if the row should be filtered, else false.
   */
  @Override
  public boolean filterRowKey(Row result) {
    if (chance < 0) {
      // with a zero chance, the rows is always excluded
      filterRow = true;
    } else if (chance > 1) {
      // always included
      filterRow = false;
    } else {
      // roll the dice
      filterRow = !(random.nextFloat() < chance);
    }
    return filterRow;
  }

  @Override
  public void reset() {
    filterRow = false;
  }


}
